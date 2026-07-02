(ns jepsen.conductor.workload.workflow
  "A workload which creates workflows, asks Conductor to launch instances of
  them, and tries to verify that each execution runs to completion."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [set :as bs]]
            [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [role :as role]]
            [jepsen.conductor.client :as c]
            [jepsen.history.fold :refer [loopf]]
            [tesser.core :as t])
  (:import (com.netflix.conductor.client.exception ConductorClientException)))

(defrecord Client [api-client task-client workflow-client workflow-executor]
  client/Client
  (open! [this test node]
    (let [api-client (c/api-client test node)]
      (assoc this
             :api-client        api-client
             :task-client       (c/task-client api-client)
             :workflow-executor (c/workflow-executor api-client)
             :workflow-client   (c/workflow-client api-client))))

  (setup! [this test]
    (with-retry [tries 10]
      (c/register-workflow!
        workflow-executor
        {:name "greetings"
         :version 1
         :tasks [{:input            {"name" "${workflow.input.name}"}
                  :name             "greet"
                  :response-timeout (:response-timeout test)
                  :ref-name         "greet_ref"
                  :type             :simple}]})
      (catch ConductorClientException e
        ; We often get BACKEND_ERROR here, so we need to retry
        (info "Setup failed due to" (.getMessage e) "- retrying")
        (when (zero? tries)
          (throw e))
        (Thread/sleep 2000)
        (retry (dec tries)))))

  (invoke! [this test op]
    (c/with-errors op
      (case (:f op)
        :start-workflow
        (let [id (c/start-workflow! workflow-client
                                    (:value op))]
          (if (nil? id)
            ; The client sometimes returns null--I think this is a bug.
            (assoc op :type :fail, :value nil, :error :nil-workflow-id)
            (assoc op :type :ok, :value id)))

        :check-workflow
        (assoc op
               :type :ok
               :value (c/get-workflow workflow-client (:value op)))

        ; We use _deciderQueue as a proxy for overall system backlog
        :queue-size
        (assoc op
               :type :ok
               :value (c/get-queue-size-for-task task-client
                                                 "_deciderQueue")))))

  (teardown! [this test])

  (close! [this test]
          (c/close! workflow-executor)
          (c/close! workflow-client)
          (c/close! api-client))

  client/Reusable
  (reusable? [this test]
    true))


;; Generators

(defrecord TrackWorkflowGen [gen completed pending]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test (assoc ctx
                                                 :completed completed
                                                 :pending pending))]
      [op (TrackWorkflowGen. gen' completed pending)]))

  (update [this test ctx event]
    (if (= :ok (:type event))
      (case (:f event)
        :start-workflow
        (let [pending' (bs/add pending (:value event))
              gen'     (gen/update gen test (assoc ctx
                                                   :completed completed
                                                   :pending pending')
                                   event)]
          (TrackWorkflowGen. gen' completed pending'))

        :check-workflow
        (let [{:keys [id status]} (:value event)]
          (case status
            ; In these cases, we assume it's done and don't need to look any
            ; more
            (:completed :failed :timed-out :terminated)
            (let [pending'   (bs/remove pending id)
                  completed' (bs/add completed id)
                  gen'       (gen/update gen test (assoc ctx
                                                         :completed completed'
                                                         :pending pending')
                                         event)]
              (TrackWorkflowGen. gen' completed' pending'))

            ; Otherwise, it's pending
            (let [pending'   (bs/add pending id)
                  completed' (bs/remove completed id)
                  gen'       (gen/update gen test (assoc ctx
                                                         :completed completed'
                                                         :pending pending')
                                         event)]
              (TrackWorkflowGen. gen' completed' pending'))))

        ; We don't care about this f
        (TrackWorkflowGen. (gen/update gen test (assoc ctx
                                                       :completed completed
                                                       :pending pending)
                                       event)
                           completed pending))
      ; Not an OK op
      (TrackWorkflowGen. (gen/update gen test (assoc ctx
                                                     :completed completed
                                                     :pending pending)
                                     event)
                         completed pending))))

(defn track-workflow-gen
  "Wraps a generator in one which keeps track of the completed and pending
  workflow IDs we know about. These are made available in nested generators via
  :completed and :pending keys in the context."
  [gen]
  (TrackWorkflowGen. gen bs/empty bs/empty))


(defn start-workflow-gen
  "Generates a series of start-workflow operations"
  []
  (for [i (range)]
    {:f     :start-workflow
     :value {:name "greetings"
             :version 1
             :input {"name" (str "jepsen " i)}}}))

(defn backpressure-interval
  "Computes a new backpressure interval in nanos given the queue size, rate
  of change of that size in 1 / ns, and current interval."
  [size size-rate interval]
  (let [nanos       1000000000
        ; We want a queue of around 100 elements
        target-size 100
        ; To get there in 10 seconds, we need a size-rate of...
        target-rate (/ (- target-size size) 10000000000)
        ; The current rate that we're adding elements to the queue
        add-rate (/ interval)
        ; size-rate = add-rate + drain-rate, so the (negative) drain rate is
        drain-rate (- size-rate add-rate)
        ; So to achieve our target rate, we need a *new* add rate.
        ; target-rate = add-rate' + drain-rate
        add-rate' (- target-rate drain-rate)
        ;_ (prn :size-rate size-rate)
        ;_ (prn :target-rate target-rate)
        ;_ (prn :add-rate add-rate :drain-rate drain-rate)
        ;_ (prn :add-rate' add-rate')
        ; But let's place some bounds on our add rate
        add-rate' (-> add-rate'
                      ; At least one per second
                      (max (/ 1 nanos))
                      ; And at most 10K/sec
                      (min (/ 10000 nanos)))
        ;_ (prn :clamped-add-rate' add-rate')
        ; Which is an interval of
        interval' (/ add-rate')
        ; Which we blend with the existing interval to smooth out fluctuations
        interval' (+ (* 1/8 interval)
                     (* 7/8 interval'))]
    (long interval')))

(defrecord BackpressureGen [; Inner generator
                            gen
                            ; Next time we can release an op
                            ^long next-time
                            ; Approximate interval between ops
                            ^long interval
                            ; Our last {:time :depth} measurement.
                            last-measurement]
  ; This is straight from gen/stagger
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (let [now       (:time ctx)
            next-time (if (= -1 next-time)
                        now
                        next-time)]
        (cond ; Pending passes through
              (identical? :pending op)
              [op this]

              ; We're ready
              (<= next-time (:time op))
              [op (BackpressureGen.
                    gen'
                    (+ (:time op) (rand/long (* 2 interval)))
                    interval
                    last-measurement)]

              ; Waiting
              true
              [(assoc op :time next-time)
                (BackpressureGen.
                  gen'
                  (+ next-time (rand/long (* 2 interval)))
                  interval
                  last-measurement)]))))

  ; When we get an :ok :queue-size, we need to update our state.
  (update [this test ctx op]
    (if (and (h/ok? op)
             (identical? :queue-size (:f op)))
      (let [measurement' {:time (:time op)
                          :size (:value op)}
            ; Current rate of change of the queue, in 1 / ns
            rate (/ (- (:size measurement')
                       (:size last-measurement))
                    (- (:time measurement')
                       (:time last-measurement)))
            interval' (backpressure-interval (:size measurement')
                                             rate
                                             interval)]
        (info "Queue size" (:value op) "so new rate is" (float (/ 1e9 interval')))
        (BackpressureGen. (gen/update gen test ctx op)
                          next-time
                          interval'
                          measurement'))
      ; Something else
      (BackpressureGen. (gen/update gen test ctx op)
                        next-time interval last-measurement))))

(defn backpressure-gen
  "Applies backpressure to a wrapped generator based on :queue-size
  operations."
  [gen]
  (BackpressureGen. gen -1 10 {:time 0, :size 0}))

(defrecord CheckGen [ctx-key]
  gen/Generator
  (op [this test ctx]
    (let [pool (get ctx ctx-key)
          n       (b/size pool)]
      (if (= 0 n)
        [:pending this]
        [(gen/fill-in-op
           {:f     :check-workflow
            :value (b/nth pool (rand/long n))}
           ctx)
         this])))

  (update [this test ctx event]
    this))

(defn check-workflow-gen
  "A generator which emits check-workflow operations for either :pending or
  :completed workflows. We use these to check on workflows throughout the
  test."
  [type]
  (CheckGen. type))

(defrecord FinalCheckGen []
  gen/Generator
  (op [this test ctx]
    (gen/op
      (->> (bs/union (:completed ctx) (:pending ctx))
           (map (fn [workflow-id]
                  {:f :check-workflow, :value workflow-id}))
           (cons (gen/log (str "Final check of " (b/size (:completed ctx))
                               " completed and " (b/size (:pending ctx))
                               " pending workflows"))))
      test ctx))

  (update [this test ctx event]
    this))

(defn final-check-gen
  "Emits one check-workflow op for each known workflow."
  []
  (FinalCheckGen.))

(def queue-size-gen
  "Checks the queue size, once per second."
  (gen/delay 1 (gen/repeat {:f :queue-size})))

(defn gen
  "Generator of operations, given CLI opts."
  [{:keys [rate]}]
  (->> (gen/any
         ; Up to one third of our requests can be starting a new workflow run.
         ; We want to make sure that the check-workflow requests can be
         ; *higher* than this, so they don't fall behind on the workflow
         ; backlog.
         (gen/stagger (/ 3 rate) (backpressure-gen (start-workflow-gen)))
         ; We want a steady stream of queue-size requests to inform the
         ; backpressure system of what's going on
         queue-size-gen
         ; Occasionally check on a workflow we think is completed, just in case
         ; it vanishes
         (gen/stagger (/ 32 rate)
                      (check-workflow-gen :completed))
         ; Otherwise, try to clear out a pending workflow
         (check-workflow-gen :pending))
       (gen/stagger (/ rate))))

(defrecord UntilDrainedGen [gen]
  gen/Generator
  (op [this test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      [op (UntilDrainedGen. gen')]))

  (update [this test ctx op]
    (if (and (h/ok? op)
             (identical? :queue-size (:f op))
             (zero? (:value op)))
      ; Done!
      nil
      ; Propagate
      (UntilDrainedGen. (gen/update gen test ctx op)))))

(defn until-drained-gen
  "Waits for the queue-size to drain to zero, then terminates."
  [gen]
  (UntilDrainedGen. gen))

;; Checkers

(defn nil-workflow-id-errors
  "Starts a task which finds a vector of error maps where a create-workflow
  request returned a nil workflow ID."
  [history]
  (h/task history nil-workflow-id []
          (->> (t/filter (comp #{:nil-workflow-id} :error))
               (t/map (fn [op']
                        {:type :nil-workflow-id
                         :op  (h/invocation history op')
                         :op' op'}))
               (t/into [])
               (h/tesser history))))

(defn concurrent-execution-errors
  "Starts a task which finds a vector of error maps wherever two processes
  executed the same task in the same workflow instance at the same time, and we
  received a completion.

  I expect this to fail; it's theoretically impossible to guarantee mutual
  exclusion. But the locks and leaase systems might make it rare!"
  [history]
  (let [fold
        (loopf {:name "concurrent-execution-errors"}
               ; Build up a map of [workflow task] -> #{[invoke-idx,
               ; complete-idx] ...}
               ([runs (b/linear bm/empty)]
                [op]
                (recur
                  (if (and (h/invoke? op)
                           (identical? :execute (:f op)))
                    (if-let [op' (h/completion history op)]
                      (let [value (:value op)
                            k [(:workflow-instance-id value)
                               (:reference-task-name value)]
                            task-runs (bm/get runs k [])]
                        (bm/put runs k (conj task-runs [(:index op)
                                                        (:index op')])))
                      runs)
                    runs))
                (b/linear runs))
               ; Merge those together
               ([runs bm/empty]
                [runs']
                ; Ugh, *another* horrible bug with reduced wrappers. Maybe an
                ; interaction with loopf?
                (let [runs  (if (reduced? runs)  @runs runs)
                      runs' (if (reduced? runs') @runs' runs')]
                  (bm/merge runs runs' into))))]
    (h/task history concurrent-execution-errors []
            (let [runs (h/fold history fold)
                  ; Sigh; there's some sort of horrible bug lurking in
                  ; jepsen.history.fold which causes this to come out reduced
                  ; on the first pass. I should dig in and fix it, but not on
                  ; the clock for this client--that basement is very deep, and
                  ; very dark, and very much full of spiders
                  runs (if (reduced? runs)
                         @runs
                         runs)]
              (->> (bm/values runs)
                   (keep (fn [intervals]
                           ; Look for an intersection
                           (loopr [a  -2
                                   a' -1]
                                  [[b b'] (sort intervals)]
                                  (if (< b a')
                                    ; Concurrent!
                                    {:type :concurrent-execution
                                     :ops  (mapv (partial h/get-index history)
                                                 (sort [a a' b b']))}
                                    (recur b b'))
                                  nil)))
                   vec)))))

(defn task-never-started-errors
  "Takes a set of started workflow IDs and a history. Starts a task which
  returns a vector of error maps for any case when a workflow was started, but
  no execution was even attempted."
  [history]
  (let [fold
        (loopf {:name "task-never-started"}
               ; Reducer
               ([workflows (b/linear bs/empty)]
                [op]
                (recur (case (:f op)
                         :start-workflow
                         (if (= :ok (:type op))
                           (bs/add workflows (:value op))
                           workflows)

                         :execute (if (h/invoke? op)
                                    (bs/remove workflows
                                               (:workflow-instance-id
                                                 (:value op)))
                                    workflows)

                         workflows))
                (b/forked workflows))
               ; Combiner
               ([workflows bs/empty]
                [workflows']
                (recur (bs/union workflows workflows'))
                (mapv (fn [workflow]
                        {:type :task-never-started
                         :workflow-id workflow})
                      workflows)))]
  (h/task history task-never-started []
          (h/fold history fold))))

(def started-workflows-fold
  "A fold which, given OK ops, finds a set of all acked started workflow IDs."
  (->> (t/filter (h/has-f? :start-workflow))
       (t/map :value)
       (t/set)))

(def workflow-status-fold
  "A fold which, given OK ops, returns a map of workflow ID to a vector of the
  statuses it took on, in OK order."
  (->> (t/filter (h/has-f? :check-workflow))
       (t/map :value)
       (t/group-by :id)
       (t/map :status)
       (t/into [])))

(def workflow-output-fold
  "A fold which, given OK ops, returns a map of workflow ID to the set of all
  outputs it produced once completed."
  (->> (t/filter (h/has-f? :check-workflow))
       (t/map :value)
       (t/group-by :id)
       (t/filter (comp #{:completed} :status))
       (t/map :output)
       (t/set)))

(defn diff-output-errors
  "Takes a map of workflow ID to the set of all outputs it produced once
  completed, and returns a collection of error maps if a single workflow ever
  produced different outputs."
  [outputs]
  (keep (fn [[id outputs]]
          (when (< 1 (count outputs))
            {:type :diff-output
             :id   id
             :outputs outputs}))
        outputs))

(defn incompatible-status-errors
  "Takes a map of workflow IDs to vectors of statuses. Returns a collection of
  error maps for any workflow which had statuses other than :running and
  :complete. Later, if we add pauses/termination, we might relax this--I'm just
  trying to get a feel for things right now."
  [statuses]
  (keep (fn [[id statuses]]
          (when (not-every? #{:running :completed} statuses)
            {:type :incompatible-status
             :id   id
             :statuses statuses}))
        statuses))

(defn incomplete-errors
  "Takes a set of started workflow IDs and a map of workflow IDs to vectors of
  statuses. Returns a collection of error maps for any workflow ID which did
  not have final status :complete."
  [started statuses]
  (->> started
       (keep (fn [id]
               (when-not (= :completed (peek (statuses id)))
                 {:type     :incomplete-at-end
                  :id       id
                  :statuses (statuses id)})))))

(defn checker-results
  "Takes a map of error types to sequences of errors, and produces a checker
  results map for it."
  [errors]
  (let [error-types (->> errors
                         (filter (comp seq val))
                         (map key)
                         sort)]
    (reduce (fn [res type]
              (let [errors (get errors type)]
                (assoc res type {:count    (count errors)
                                 :examples (take 16 errors)})))
            {:valid?      (not (seq error-types))
             :error-types error-types}
            error-types)))

(defn final-status-counts
  "Takes a map of workflow ID to vectors of statuses. Returns a frequency map
  of the final status for each workflow ID."
  [statuses]
  (->> (vals statuses)
       (map peek)
       frequencies))

(defrecord CompletionChecker []
  checker/Checker
  (check [this test history opts]
    (h/ensure-pair-index history)
    (let [concurrent-execution-errors (concurrent-execution-errors history)
          nil-workflow-id-errors      (nil-workflow-id-errors history)
          task-never-started-errors   (task-never-started-errors history)
          {:keys [started status output]}
          (->> (t/filter h/ok?)
               (t/fuse
                 {:started started-workflows-fold
                  :status  workflow-status-fold
                  :output  workflow-output-fold})
               (h/tesser history))]
      (assoc
        (checker-results
          {:concurrent-execution @concurrent-execution-errors
           :diff-output         (diff-output-errors output)
           :incomplete          (incomplete-errors started status)
           :incompatible-status (incompatible-status-errors status)
           :nil-workflow-id     @nil-workflow-id-errors
           :task-never-started  @task-never-started-errors})
        :started-count (count started)
        :final-status-counts (final-status-counts status)))))

(defn completion-checker
  "This checker detects several types of errors

      :concurrent-execution For a single workflow instance and task reference
                            name, two workers executed that task concurrently.

      :diff-output          A workflow execution had two different outputs
                            while in the completed state.

      :incomplete           A started workflow execution was not complete at
                            the end of the test.

      :incompatible-status  A workflow execution had a status we did not
                            expect.

      :nil-workflow-id      A create-workflow request returned nil, instead of
                            an ID string.

      :task-never-started   A workflow was started but no worker was ever asked
                            to attempt a task for it."
  []
  (CompletionChecker.))

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  {:client              (role/restrict-client :server (map->Client {}))
   :checker             (completion-checker)
   :wrap-generator      track-workflow-gen
   :generator           (gen opts)
   :recovery-generator  (until-drained-gen
                          (gen/any
                            queue-size-gen
                            (check-workflow-gen :pending)))
   :final-generator     [(gen/any
                           (until-drained-gen queue-size-gen)
                           (final-check-gen))
                         ; One final peek
                         (gen/limit 1 queue-size-gen)]})

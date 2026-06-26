(ns jepsen.conductor.workload.workflow
  "A workload which creates workflows, asks Conductor to launch instances of
  them, and tries to verify that each execution runs to completion."
  (:require [bifurcan-clj [core :as b]
                          [set :as bs]]
            [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]
                    [role :as role]]
            [jepsen.conductor.client :as c]
            [tesser.core :as t]))

(defrecord Client [api-client task-client workflow-client workflow-executor]
  client/Client
  (open! [this test node]
    (let [api-client (c/api-client test node)]
      (assoc this
             :api-client        api-client
             :workflow-executor (c/workflow-executor api-client)
             :workflow-client   (c/workflow-client api-client))))

  (setup! [this test]
    (c/register-workflow!
      workflow-executor
      {:name "greetings"
       :version 1
       :tasks [{:type :simple
                :name "greet"
                :ref-name "greet_ref"
                :input {"name" "${workflow.input.name}"}}]}))

  (invoke! [this test op]
    (c/with-errors op
      (case (:f op)
        :start-workflow
        (assoc op
               :type :ok
               :value (c/start-workflow! workflow-client
                                         (:value op)))

        :check-workflow
        (assoc op
               :type :ok
               :value (c/get-workflow workflow-client (:value op))))))

  (teardown! [this test])

  (close! [this test]
          (c/close! workflow-executor)
          (c/close! workflow-client)
          (c/close! api-client)))


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
        this)
      ; Not an OK op
      this)))

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

;; Checkers

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
    (let [{:keys [started status output]}
          (->> (t/filter h/ok?)
               (t/fuse
                 {:started started-workflows-fold
                  :status  workflow-status-fold
                  :output  workflow-output-fold})
               (h/tesser history))]
      (assoc
        (checker-results
          {:diff-output (diff-output-errors output)
           :incomplete  (incomplete-errors started status)
           :incompatible-status (incompatible-status-errors status)})
        :started-count (count started)
        :final-status-counts (final-status-counts status)))))

(defn completion-checker
  "This checker detects several types of errors

      :diff-output          A workflow execution had two different outputs
                            while in the completed state.
      :incomplete           A started workflow execution was not complete at
                            the end of the test.
      :incompatible-status  A workflow execution had a status we did not
                            expect."
  []
  (CompletionChecker.))

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  {:client          (role/restrict-client :server (map->Client {}))
   :checker         (completion-checker)
   :wrap-generator  track-workflow-gen
   :generator       (gen/any
                      (gen/stagger (/ 2 (:rate opts))
                                   (start-workflow-gen))
                      (gen/stagger (/ 32 (:rate opts))
                                          (check-workflow-gen :completed))
                      (check-workflow-gen :pending))
   :final-generator (final-check-gen)})

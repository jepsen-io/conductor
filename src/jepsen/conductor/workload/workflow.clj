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
                    [random :as rand]
                    [role :as role]]
            [jepsen.conductor.client :as c]))

(defrecord Client [api-client task-client workflow-client workflow-executor]
  client/Client
  (open! [this test node]
    (let [api-client (c/api-client test node)]
      (assoc this
             :api-client        api-client
             :workflow-executor (c/workflow-executor api-client)
             :workflow-client   (c/workflow-client api-client))))

  (setup! [this test]
    (c/register-workflow! workflow-executor))

  (invoke! [this test op]
    (case (:f op)
      :start-workflow
      (assoc op
             :type :ok
             :value (c/start-workflow! workflow-client
                                       (:value op)))

      :check-workflow
      (assoc op
             :type :ok
             :value (c/get-workflow workflow-client (:value op)))))

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

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  {:client          (role/restrict-client :server (map->Client {}))
   :checker         (checker/unbridled-optimism)
   :wrap-generator  track-workflow-gen
   :generator       (gen/any
                      (gen/stagger (/ 10 (:rate opts))
                                   (start-workflow-gen))
                      (gen/stagger (/ 100 (:rate opts))
                                          (check-workflow-gen :completed))
                      (check-workflow-gen :pending))
   :final-generator (final-check-gen)})

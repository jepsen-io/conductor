(ns jepsen.conductor.workload.workflow
  "A workload which creates workflows, asks Conductor to launch instances of
  them, and tries to verify that each execution runs to completion."
  (:require [clojure.tools.logging :refer [info warn]]
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
             :value (c/workflow-status workflow-client (:value op)))))

  (teardown! [this test])

  (close! [this test]
          (c/close! workflow-executor)
          (c/close! workflow-client)
          (c/close! api-client)))

(defn start-workflow-gen
  "Generates a series of start-workflow operations"
  []
  (for [i (range)]
    {:f     :start-workflow
     :value {:name "greetings"
             :version 1
             :input {"name" (str "jepsen " i)}}}))

(defrecord CheckWorkflowGen [pending]
  gen/Generator
  (op [this test ctx]
    (if (seq pending)
      [(gen/fill-in-op
         {:f     :check-workflow
          :value (rand/nth pending)}
         ctx)
       this]
      [:pending this]))

  (update [this test ctx event]
    (if (and (= :ok (:type event))
             (= :start-workflow (:f event)))
      (update this :pending conj (:value event))
      this)))

(defn check-workflow-gen
  "A generator which listens for successful :start-workflow operations and
  keeps track of their IDs, then periodically emits :check-workflow operations
  for them."
  []
  (CheckWorkflowGen. []))

(defn gen
  "Generator of workflow ops"
  [opts]
  (gen/any
    (gen/stagger (* 10 (/ (:rate opts))) (start-workflow-gen))
    (check-workflow-gen)))

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  {:client          (role/restrict-client :server (map->Client {}))
   :checker         (checker/unbridled-optimism)
   :generator       (gen opts)
   :final-generator nil})

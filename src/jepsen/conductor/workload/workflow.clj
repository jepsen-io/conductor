(ns jepsen.conductor.workload.workflow
  "A workload which creates workflows, asks Conductor to launch instances of
  them, and tries to verify that each execution runs to completion."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [role :as role]
                    [generator :as gen]]))

(defrecord Client []
  client/Client
  (open! [this test node]
    this)

  (setup! [this test])

  (invoke! [this test op])

  (teardown! [this test])

  (close! [this test]
    ))

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  {:client          (role/restrict-client :server (map->Client {}))
   :checker         (checker/unbridled-optimism)
   :generator       nil
   :final-generator nil})

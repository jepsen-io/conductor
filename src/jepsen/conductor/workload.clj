(ns jepsen.conductor.workload
  "A workload which creates workflows, asks Conductor to launch instances of
  them, and tries to verify that each execution runs to completion."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [set :as bs]]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure [pprint :refer [pprint]]
                     [walk :as walk]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr with-retry]]
            [jepsen [client :as client]
                    [random :as rand]
                    [role :as role]
                    [store :as store]]
            [jepsen.conductor [client :as c]
                              [generator :as generator]]
            [jepsen.conductor.checker.workflow :refer [checker]]
            [jepsen.history.fold :refer [loopf]]
            [tesser.core :as t])
  (:import (com.netflix.conductor.client.exception ConductorClientException)
           (clojure.lang ExceptionInfo)))

(defn snarf-workflow-json!
  "Downloads the JSON for a single workflow."
  [test id]
  (with-open [w (io/writer (store/path! test "workflows" (str id ".json")))]
    (let [node (-> test :roles :server rand/nth)
          url  (str "http://" node ":8080/api/workflow/" id
                    "?includeTasks=true")
          res (http/get url {:as :stream})
          json (json/parse-stream (io/reader (:body res)))]
      (json/generate-stream json w {:pretty true
                                    :throw-entire-message? true}))))

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

        :running-workflows
        (assoc op
               :type :ok
               :value (c/get-running-workflows workflow-client
                                               "greetings"
                                               1))

        :check-workflow
        (assoc op
               :type :ok
               :value (c/get-workflow workflow-client (:value op)))

        :snarf-workflow
        (try
          (snarf-workflow-json! test (:value op))
          (assoc op :type :ok)
          (catch ExceptionInfo e
            (assoc op
                   :type :fail
                   :error (select-keys (ex-data e)
                                       [:status :headers :body]))))

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

(defn workload
  "Constructs a workflow workload given CLI options."
  [opts]
  (merge (generator/workload opts)
         {:client  (role/restrict-client :server (map->Client {}))
          :checker (checker)}))

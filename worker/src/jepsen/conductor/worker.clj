(ns jepsen.conductor.worker
  (:gen-class)
  (:require [clojure.tools.logging :refer [info warn fatal]]
            [unilog.config :as unilog])
  (:import (com.netflix.conductor.client.automator TaskRunnerConfigurer
                                                   TaskRunnerConfigurer$Builder)
           (com.netflix.conductor.client.http TaskClient)
           (com.netflix.conductor.client.worker Worker)
           (com.netflix.conductor.common.metadata.tasks Task
                                                        TaskResult
                                                        TaskResult$Status)
           (io.orkes.conductor.client ApiClient)))

(deftype AnWorker []
  Worker
  (getTaskDefName [_]
    "greet")

  (execute [_ task]
    (let [name (.. task getInputData (get "name"))
          res  (TaskResult. task)]
      (.setStatus res TaskResult$Status/COMPLETED)
      (.addOutputData res "greeting" (str "hello, " name))
      res)))

(defn api-client
  "Opens a new Conductor API client."
  []
  (let [url (System/getenv "CONDUCTOR_SERVER_URL")]
    (when-not url
      (throw (ex-info "No CONDUCTOR_SERVER_URL env var"
                      {:type :no-conductor-server-url})))
    (ApiClient. url nil nil)))

(defn -main
  "Opens a client, sets up a task runner, and... starts it?"
  [& args]
  (try
    (unilog/start-logging! {:level :info, :console true})
    (let [api-client (api-client)
          task-client (TaskClient. api-client)
          trc (.. (TaskRunnerConfigurer$Builder. task-client
                                                 [(AnWorker.)])
                  (withThreadCount 10)
                  (build))]
      (.init trc))
    (catch Throwable t
      (fatal t "Worker threw!")
      (System/exit 1))))

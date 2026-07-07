(ns jepsen.conductor.worker
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn fatal]]
            [jepsen.random :as rand]
            [org.httpkit.client :as http]
            [unilog.config :as unilog])
  (:import (com.netflix.conductor.client.automator TaskRunnerConfigurer
                                                   TaskRunnerConfigurer$Builder)
           (com.netflix.conductor.client.http TaskClient)
           (com.netflix.conductor.client.worker Worker)
           (com.netflix.conductor.common.metadata.tasks Task
                                                        TaskResult
                                                        TaskResult$Status)
           (io.orkes.conductor.client ApiClient)
           (java.io FileNotFoundException)))

(def node
  "The current node name."
  (delay
    (or (System/getenv "JEPSEN_NODE")
        (throw (ex-info "No JEPSEN_NODE env var" {})))))

(def incarnation
  "An integer representing the number of times this process has run. We use
  this to distinguish between multiple runs of the worker as it's killed."
  (let [file (io/file "incarnation")]
    (delay
      (try (inc (parse-long (slurp file)))
           (catch FileNotFoundException e
             ; This is technically a race condition but I am very confident we
             ; can't kill/restart processes fast enough to trigger it. If we
             ; did, the history will start screaming at us about processes
             ; doing two things at once.
             (spit file "0")
             0)))))

(def thread->id
  "A map of thread IDs to small integers. We use this to shorten our process
  IDs."
  (atom {}))

(defn process
  "Returns a process ID."
  []
  (let [tid (.getId (Thread/currentThread))
        id  (or (get @thread->id tid)
                (do (swap! thread->id
                           (fn [thread->id]
                             (or (get thread->id tid)
                                 (assoc thread->id tid (count thread->id)))))
                    (get @thread->id tid)))]
    (keyword (str @node "." @incarnation "." id))))

(defn log-op!
  "Logs an operation to the history."
  [history-url op]
  @(http/post history-url
                  {:headers {"Content-Type" "application/edn"}
                   :body (pr-str (assoc op :process (process)))}))

(deftype AnWorker [history-url] ; The URL we'll post our history events to.
  Worker
  (getTaskDefName [_]
    "greet")

  (execute [_ task]
    ; Record that we're starting invocation
    (let [; We use this to suppress multiple complete ops
          complete? (atom false)
          ; We want nondeterministic output here; it's what will let us
          ; determine whether Conductor provides determinism.
          output    {"out" (rand/long)}]
      (try
        (log-op! history-url
                 {:type    :invoke
                  :f       :execute
                  :value   {:reference-task-name  (.getReferenceTaskName task)
                            :workflow-instance-id (.getWorkflowInstanceId task)
                            :task-id              (.getTaskId task)
                            :input                (into {} (.getInputData task))
                            :output               output}})

        (info (str "Begin task (ref " (.getReferenceTaskName task)
                   ", task id " (.getTaskId task) ")"))

        ; Pretend to do work. We intentionally want to trigger the timeout
        ; sometimes.
        (Thread/sleep (rand/zipf 2.0 30000))
        (let [res (TaskResult. task)]
          (.setStatus res TaskResult$Status/COMPLETED)
          (doseq [[k v] output]
            (.addOutputData res k v))
          (info (str "Complete task (ref " (.getReferenceTaskName task)
                     ", task id " (.getTaskId task) ")"))

          ; Record that we're done
          (reset! complete? true)
          (log-op! history-url
                   {:type    :ok
                    :f       :execute
                    :value   nil})
          res)
        (catch Throwable t
          ; If anything happens, we really do want to try and let the history
          ; know about it.
          (when-not @complete?
            (log-op! history-url
                     {:type   :info
                      :f      :execute
                      :value  nil
                      :error  (str (.getClass t) ": " (.getMessage t))}))
          (throw t)))))

  (onErrorUpdate [_ task]
    (warn "onErrorUpdate task (ref " (.getReferenceTaskName task)
          ", task id " (.getTaskId task) ")"))

  (getPollingInterval [_]
    ; Poll every 100 ms
    100))

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
    @node ; Fail fast if missing
    (let [api-client (api-client)
          task-client (TaskClient. api-client)
          history-url (or (System/getenv "JEPSEN_HISTORY_URL")
                          (throw (ex-info "No JEPSEN_HISTORY_URL env var" {})))
          worker (AnWorker. history-url)
          trc (.. (TaskRunnerConfigurer$Builder. task-client [worker])
                  (withThreadCount 128)
                  ; There's also withTaskToThreadCount and withTaskThreadCount,
                  ; but I can't tell what they actually do just yet

                  ; This is in millis, and controls the time the worker will
                  ; block awaiting a queue item.
                  ;(withTaskPollTimeout ???)
                  (build))]
      (.init trc))
    (catch Throwable t
      (fatal t "Worker threw!")
      (System/exit 1))))

(ns jepsen.conductor.client
  "Wrapper for the Conductor Java client."
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.conductor [base :refer :all]])
  (:import (io.orkes.conductor.client ApiClient)
           (com.netflix.conductor.client.http TaskClient
                                              WorkflowClient)
           (com.netflix.conductor.common.metadata.workflow StartWorkflowRequest)
           (com.netflix.conductor.common.run Workflow
                                             Workflow$WorkflowStatus)
           (com.netflix.conductor.sdk.workflow.def ConductorWorkflow)
           (com.netflix.conductor.sdk.workflow.def.tasks SimpleTask)
           (com.netflix.conductor.sdk.workflow.executor WorkflowExecutor)))

(defn api-client*
  "Opens an API client to the given server URL."
  [test server-url]
  ; I think we can just skip the auth key and secret?
  (ApiClient. server-url nil nil))

(defn api-client
  "Opens an API client *from* the given node in the test. This will pick one of
  the server nodes in the test based on the index of the given node."
  [test node]
  (api-client* test (worker-server-url test node)))

(defn workflow-executor
  "Creates a WorkflowExecutor."
  [^ApiClient api-client]
  ; What is this number?
  (WorkflowExecutor. api-client 10))

(defn workflow-client
  "Opens a WorkflowClient."
  [^ApiClient api-client]
  (WorkflowClient. api-client))

(defn task-client
  "Opens a TaskClient"
  [^ApiClient api-client]
  (TaskClient. api-client))

(defprotocol Close
  (close! [this] "Closes a client."))

(extend-protocol Close
  ApiClient        (close! [c] (.shutdown c))
  WorkflowExecutor (close! [e] (.shutdown e))
  WorkflowClient   (close! [c] (.close c))
  ; I can't figure out if there is a way to close this
  TaskClient       (close [c]))

; Define workflows

(defn register-workflow!
  "Registers a workflow with the server."
  [^WorkflowExecutor exec]
  (let [workflow (doto (ConductorWorkflow. exec)
                  (.setName "greetings")
                  (.setVersion 1))
        ; I think this is the name and reference name
        task (doto (SimpleTask. "greet" "greet_ref")
               (.input "name" "${workflow.input.name}"))
        ; Add it to the workflow
        _ (.add workflow task)
        ; And register the workflow--dunno what the trues are.
        _ (.registerWorkflow workflow true true)]))

(defn start-workflow!
  "Starts a workflow... execution? I think? Returns the string workflow ID.
  Options:

    :name     The workflow name
    :version  The version of that workflow
    :input    An input map"
  [^WorkflowClient client workflow]
  (let [req (doto (StartWorkflowRequest.)
              (.setName (:name workflow))
              (.setVersion (int (:version workflow)))
              (.setInput (:input workflow)))
        workflow-id (.startWorkflow client req)]
    workflow-id))

(defn get-workflow
  "Asks a WorkflowClient to fetch a workflow by string ID."
  [^WorkflowClient client ^String workflow-id]
  (.getWorkflow client workflow-id true))

(defn workflow-status
  "Returns the current status of a workflow as a keyword."
  [workflow-client workflow-id]
  (condp identical? (.getStatus (get-workflow workflow-client workflow-id))
    Workflow$WorkflowStatus/RUNNING    :running
    Workflow$WorkflowStatus/COMPLETED  :completed
    Workflow$WorkflowStatus/FAILED     :failed
    Workflow$WorkflowStatus/TIMED_OUT  :timed-out
    Workflow$WorkflowStatus/TERMINATED :terminated
    Workflow$WorkflowStatus/PAUSED     :paused))

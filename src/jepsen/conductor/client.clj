(ns jepsen.conductor.client
  "Wrapper for the Conductor Java client."
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.core.protocols :refer [Datafiable]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.conductor [base :refer :all]])
  (:import (io.orkes.conductor.client ApiClient)
           (com.netflix.conductor.client.exception ConductorClientException)
           (com.netflix.conductor.client.http TaskClient
                                              WorkflowClient)
           (com.netflix.conductor.common.metadata.tasks PollData
                                                        TaskDef)
           (com.netflix.conductor.common.metadata.workflow StartWorkflowRequest)
           (com.netflix.conductor.common.run Workflow
                                             Workflow$WorkflowStatus)
           (com.netflix.conductor.sdk.workflow.def ConductorWorkflow)
           (com.netflix.conductor.sdk.workflow.def.tasks SimpleTask)
           (com.netflix.conductor.sdk.workflow.executor WorkflowExecutor)))

; Coercing types
(extend-protocol Datafiable
  Workflow$WorkflowStatus
  (datafy [status]
    (condp identical? status
      Workflow$WorkflowStatus/RUNNING    :running
      Workflow$WorkflowStatus/COMPLETED  :completed
      Workflow$WorkflowStatus/FAILED     :failed
      Workflow$WorkflowStatus/TIMED_OUT  :timed-out
      Workflow$WorkflowStatus/TERMINATED :terminated
      Workflow$WorkflowStatus/PAUSED     :paused))

  Workflow
  (datafy [w]
    {:id         (.getWorkflowId w)
     :name       (.getWorkflowName w)
     :version    (.getWorkflowVersion w)
     :priority   (.getPriority w)
     :status     (datafy (.getStatus w))
     :input      (into (sorted-map) (datafy (.getInput w)))
     :output     (into (sorted-map) (datafy (.getOutput w)))
     :start-time (.getStartTime w)}))

; Opening clients

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

(defn task
  "Takes a map and constructs a Task. Keys are:

      :type       The type of task. Right now this is just :simple for
                  SimpleTask.
      :name       The short name for the task.
      :ref-name   The reference name for the task.
      :input      A map of input name (e.g. \"prey\") to expression (e.g.
                  \"${rabbit.output.location}\").
      :poll-timeout The poll timeout, in seconds.
      :timeout    The task timeout, in seconds. I'm not sure what this is.
      :total-timeout The, uh... total timeout? In seconds.
      :response-timeout How long before we consider this task timed out, in
                        seconds.
      :retry-count  The number of allowable retries
      :retry-delay  How long to wait befor retrying, in seconds"
  [{:keys [type input poll-timeout name ref-name response-timeout retry-count
           retry-delay timeout total-timeout] :as task
    :or {; The default timeout, one hour, is far longer than we're willing to
         ; wait during most tests.
         response-timeout     10
         ; We're trying to figure out if Conductor will ensure work happens,
         ; so it doesn't make any sense for it to give up on a task.
         retry-count          Integer/MAX_VALUE
         retry-delay          5}}]
  (case type
    :simple (let [t (SimpleTask. name ref-name)]
              (doseq [[name expr] input]
                (.input t ^String name expr))
              (.setTaskDef
                t
                (cond-> (TaskDef/builder)
                  true             (.name name)
                  poll-timeout     (.pollTimeoutSeconds poll-timeout)
                  response-timeout (.responseTimeoutSeconds response-timeout)
                  retry-count      (.retryCount retry-count)
                  retry-delay      (.retryDelaySeconds retry-delay)
                  timeout          (.timeoutSeconds timeout)
                  total-timeout    (.totalTimeoutSeconds total-timeout)
                  true             (.build)))
              t)))

(defn ^ConductorWorkflow workflow
  "Takes a Clojure structure representing a workflow and returns a
  ConductorWorkflow from it. The workflow should be a map with the following
  keys:

      :name     String name of the workflow
      :version  Version number
      :tasks    A collection of task maps.

  For the task maps, see `task`."
  [^WorkflowExecutor exec workflow]
  (let [w (doto (ConductorWorkflow. exec)
            (.setName (:name workflow))
            (.setVersion (:version workflow)))]
    (doseq [t (:tasks workflow)]
      (.add w (task t)))
    w))

(defn register-workflow!
  "Registers a workflow with the server. See `workflow` for the definition of a
  workflow map."
  [workflow-exec workflow-map]
  (.registerWorkflow (workflow workflow-exec workflow-map) true true))

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
  "Asks a WorkflowClient to fetch a workflow by string ID. Returns `{:id id,
  :status :not-found}` if the workflow is not found; we do this so that the
  existing checking machinery which looks at statuses can work seamlessly here
  too."
  [^WorkflowClient client ^String workflow-id]
  (try (datafy (.getWorkflow client workflow-id true))
       (catch ConductorClientException e
         (if (re-find #"Workflow .+ not found" (.getMessage e))
           {:id     workflow-id
            :status :not-found}
           (throw e)))))

(defn get-all-poll-data
  "Gets the last poll data for all task types."
  [^TaskClient task-client]
  (datafy (.getAllPollData task-client)))

(defn get-queue-size-for-task
  "Fetches the size of the given queue."
  [^TaskClient task-client ^String task-type]
  (.getQueueSizeForTask task-client task-type))

;; Error handling

(defmacro with-errors
  "Takes an operation and a body. Evaluates body, converting common exceptions
  to :info or :fail ops."
  [op & body]
  `(try ~@body
        (catch ConductorClientException e#
          (let [m# (.getMessage e#)]
            (condp re-find m#
              ; Huh.
              #"BACKEND_ERROR"
              (assoc ~op :type :info, :error [:backend-error m#])

              #"Connection reset"
              (assoc ~op :type :info, :error :connection-reset)

              #"^Failed to connect to"
              (assoc ~op :type :fail, :error :failed-to-connect)

              #"^Internal Server Error"
              (assoc ~op :type :info, :error :internal-server-error)

              #"timeout"
              (assoc ~op :type :info, :error :timeout)

              (throw e#))))))

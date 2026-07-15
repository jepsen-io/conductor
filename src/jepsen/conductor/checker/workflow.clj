(ns jepsen.conductor.checker.workflow
  "This namespace checks the execution of a single workflow."
  (:refer-clojure :exclude [merge flush])
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [walk :as walk]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [history :as h]
                    [store :as store]]
            [jepsen.history.fold :refer [loopf]]
            [potemkin :refer [definterface+]]
            [tesser.core :as t])
  (:import (jepsen.history Op)))

(defn earliest-index
  "Takes two integers and returns the minimum one which is *not* -1, or -1 if
  both are."
  [^long a, ^long b]
  (cond (= -1 a)  b
        (= -1 b)  a
        true      (min a b)))

(defn by-earliest-index
  "Takes two index integers i1 and i2, and a pair of values v1 and v2. Returns
  one of v1 or v2 depending on whether i1 or i2 is the earliest index,
  consistent with earliest-index."
  [^long i1, ^long i2, v1, v2]
  (cond (= -1 i1) v2
        (= -1 i2) v1
        (< i1 i2) v1
        true      v2))

(defn get-index
  "Like history/get-index, but returns nil for -1. We use primitive longs with
  -1 as an index sentinel value all over the place, so this is handy."
  [history ^long index]
  (when (not= -1 index)
    (h/get-index history index)))

(definterface+ Model
  (step [model history ^Op op]
        "Applies a single operation to a model, returning a new model.")

  (finalize [model history]
         "Performs a final check once all operations have been incorporated.
         Returns a Bifurcan set of error types.")

  (merge [model1 model2 history]
         "Merges two models together.")

  (flush-1 [model history]
         "Applies pending operations to a model, returning a new model. Only
         one pass."))

(defn fixed-point
  "Calls f on x until x is no longer identical."
  [f x]
  (let [x' (f x)]
    (if (identical? x x')
      x
      (recur f x'))))

(defn flush
  "Calls flush-1 repeatedly until no longer identical."
  [model history]
  (fixed-point #(flush-1 % history) model))

(defn step+
  "Like step, but if the model returns ::pending, adds the op to the :pending
  set."
  [model history op]
  (let [model' (step model history op)]
    (if (identical? ::pending model')
      (update model :pending bs/add op)
      model')))

(defrecord Task [; The index of the input op. -1 initially.
                 ^long input-index
                 ; The task input
                 input
                 ; The set of task outputs produced by various executions
                 outputs
                 ; A set of errors
                 errors
                 ]
  Model
  (step [this history op]
    (case (.f op)
      :execute
      (if (h/invoke? op)
        (let [value   (.value op)
              outputs (bs/add outputs (:output value))]
          (cond ; First observation of an input
                (= -1 input-index)
                (Task. (.index op)
                       (:input value)
                       outputs
                       errors)

                ; Different input
                (not= input (:input value))
                (let [op1 (h/get-index history input-index)
                      op1' (when (h/invoke? op1)
                             (h/completion history op1))]
                  (Task. input-index
                         input
                         outputs
                         (bs/add errors
                                 (cond->
                                   {:type      :different-task-input
                                    :expected input
                                    :actual   (:input value)
                                    :op1      (h/get-index history input-index)
                                    :op2      op}
                                   op1' (assoc :op1' op1')))))

                ; Fine; this is consistent
                true
                (Task. input-index
                       input
                       outputs
                       errors)))

        ; OK/info/fail
        this)

      ; Some other f
      this))

  (merge [a b history]
    (let [errors
          (cond-> (bs/union errors (:errors b))
            ; We found different inputs
            (and (not= -1 input-index)
                 (not= -1 (:input-index b))
                 (not= input (:input b)))
            (bs/add {:type     :different-task-input
                     :expected input
                     :actual   (:input b)
                     :op1      (h/get-index history input-index)
                     :op2      (h/get-index history (:input-index b))}))]
          (Task. (earliest-index input-index (:input-index b))
                 (by-earliest-index input-index (:input-index b)
                                    input (:input b))
                 (bs/union outputs (:outputs b))
                 errors)))

  (finalize [this history]
    (cond-> errors
      (zero? (b/size outputs))
      (bs/add {:type :task-never-executed}))))

(defn check-missing
  "Takes a Workflow, a history, and a failed read. Tries to figure out
  if the failed read would have been a stale read (assuming Workflows
  are linearizable), and if so, returns workflow with a :missing error.
  Otherwise, returns workflow unchanged."
  [workflow history op]
  (let [start  (h/get-index history (:start-index workflow))
        start' (h/completion history start)
        invoke (h/invocation history op)]
    (if (or ; We started this read after the workflow was acknowledged as
            ; started
            (and (h/ok? start')
                 (< (:index start') (:index invoke)))
            ; We started this read after we learned the output, which
            ; *must* have been an OK completion op.
            (and (not= -1 (:output-index workflow))
                 (< (:output-index workflow) (:index invoke)))
            ; We could be more precise here (e.g. execute) but this
            ; should hopefully be enough
            )
      (update workflow :errors bs/add
              {:type        :missing
               :workflow-id (:id workflow)
               :start       start
               :start'      start'
               :read        invoke
               :read'       op})
      ; Fine
      workflow)))

(defrecord Workflow [; The workflow run id
                     id
                     ; The index of the start-workflow invocation.
                     ^long start-index
                     ; Did the start op fail?
                     ^boolean start-failed?
                     ; The index of the op that told us the input
                     ^long input-index
                     ; The input to the workflow
                     input
                     ; The index of the op that first told us the output
                     ^long output-index
                     ; The output of the workflow
                     output
                     ; The last observed check-workflow :ok op
                     ^long last-check-index
                     ; A map of task reference name to Task state
                     tasks
                     ; A set of unapplied operations. We buffer ops here when
                     ; we haven't learned the information we need to check
                     ; them.
                     pending
                     ; A set of errors
                     errors
                     ]
  Model
  (step [this history op]
    (case (.f op)
      :start-workflow
      (case (.type op)
        :invoke
        (if (= -1 input-index)
          (assoc this
                 :start-index (.index op)
                 :input-index (.index op)
                 :input (:input (.value op)))
          ; The only way we can get here is if we started the same workflow ID
          ; twice!
          (let [op1  (h/get-index history start-index)
                op1' (h/completion history op1)
                op2' (h/completion history op)]
            (assoc this
                   :errors (bs/add errors
                                   {:type        :duplicate-workflow-id
                                    :workflow-id id
                                    :op1         op1
                                    :op1'        op1'
                                    :op2         op
                                    :op2'        op2'}))))

        ; We need this to find G1a
        :fail (assoc this :start-failed? true)

        this)

      :check-workflow
      (let [value (.value op)]
        (cond ; Not an OK op? Nothing to learn.
              (not (h/ok? op))
              this

              ; We haven't seen the start yet; come back later
              (= -1 start-index)
              ::pending

              ; Not-found? This might be nonlinearizable.
              (identical? :not-found (:status value))
              (check-missing this history op)

              ; OK, figure out our inputs and outputs and errors
              true
              (let [new-input?  (and (= -1 input-index)
                                     (not (nil? (:input value))))
                    new-output? (and (= -1 output-index)
                                     ; While running, workflows may have
                                     ; intermediate outputs like {}. We only
                                     ; check the completed states.
                                     (identical? :completed (:status value)))
                    ; Learn input and output
                    input-index'  (if new-input? (.index op) input-index)
                    input'        (if new-input? (:input value) input)
                    output-index' (if new-output? (.index op) output-index)
                    output'       (if new-output? (:output value) output)
                    errors (cond-> errors
                             ; G1a
                             start-failed?
                             (bs/add
                               (let [start (h/get-index history start-index)]
                                 {:type :g1a
                                  :workflow-id id
                                  :start  start
                                  :start' (h/completion history start)
                                  :op     op}))

                             ; Different input
                             (when-let [i (:input value)]
                               (and (not= -1 input-index)
                                    (not= input' i)))
                             (bs/add
                               {:type        :different-workflow-input
                                :workflow-id id
                                :expected    input
                                :actual      (:input value)
                                :op1         (h/get-index history input-index)
                                :op2         op})

                             ; Different output
                             (when-let [o (:output value)]
                               (and (not= -1 output-index)
                                    (identical? :completed (:status value))
                                    (not= output' o)))
                             (bs/add
                               {:type        :different-workflow-output
                                :workflow-id id
                                :expected    output
                                :actual      (:output value)
                                :op1         (h/get-index history output-index)
                                :op2         op}))
                    last-check-index' (max last-check-index (:index op))]
                (Workflow.
                  id
                  start-index
                  start-failed?
                  input-index'
                  input'
                  output-index'
                  output'
                  last-check-index'
                  tasks
                  pending
                  errors))))

      :execute
      (cond
        ; We don't care about completions
        (not (h/invoke? op))
        this

        ; Not ready
        (= -1 input-index)
        ::pending

        true
        ; TODO: later we'll have a more sophisticated task mapping, but right
        ; now let's assume every task takes the workflow input as its input.
        (let [value     (.value op)
              ref-name  (:reference-task-name value)
              task      (or (bm/get tasks ref-name)
                            (Task. input-index input bs/empty bs/empty))
              task'     (step task history op)
              errors'   (cond-> errors
                          ; G1a
                          start-failed?
                          (bs/add
                            (let [start (h/get-index history start-index)]
                              {:type        :g1a
                               :workflow-id id
                               :start       start
                               :start'      (h/completion history start)
                               :op          op})))]
            (assoc this
                   :tasks (bm/put tasks ref-name task')
                   :errors errors')))

      :snarf-workflow
      (case (:type op)
        :invoke this
        :ok     this
        ; This may be overly conservative, but let's complain if the
        ; snarf-workflow fails for *any* reason--it only happens during the
        ; final phase, so every server should have been up for a while. I'm
        ; very confident it shouldn't return 404, but (e.g.) a 503 would also
        ; be weird.
        (check-missing this history op))

      ; Some other :f
      this))

  (flush-1 [this history]
    (loopr [model    this
            pending' pending]
           [op pending]
           (let [model' (step model history op)]
             (if (identical? ::pending model')
               ; Still pending
               (recur model pending')
               ; Applied
               (recur model' (bs/remove pending' op))))
           (if (not= (b/size pending) (b/size pending'))
             ; We changed!
             (do (assoc model :pending pending'))
             ; Unchanged
             this)))

  (merge [a b history]
    (let [errors'
          (cond-> (bs/union errors (:errors b))
            ; Different starts!?
            (and (not= -1 start-index)
                 (not= -1 (:start-index b))
                 (not= start-index (:start-index b)))
            (bs/add
              (let [op1  (h/get-index history start-index)
                    op1' (h/completion history op1)
                    op2  (h/get-index history (:start-index b))
                    op2' (h/completion history op2)]
                {:type        :duplicate-workflow-id
                 :workflow-id id
                 :op1         op1
                 :op1'        op1'
                 :op2         op2
                 :op2'        op2'}))

            ; Different inputs
            (and (not= -1 input-index)
                 (not= -1 (:input-index b))
                 (not= input (:input b)))
            (bs/add {:type      :different-workflow-input
                     :workflow-id id
                     :expected  input
                     :actual    (:input b)
                     :op1       (h/get-index history input-index)
                     :op2       (h/get-index history (:input-index b))})

            ; Different outputs
            (and (not= -1 output-index)
                 (not= -1 (:output-index b))
                 (not= output (:output b)))
            (bs/add {:type :different-workflow-output
                     :workflow-id id
                     :expected output
                     :actual (:output b)
                     :op1 (h/get-index history output-index)
                     :op2 (h/get-index history (:output-index b))}))
          ; Merge tasks
          tasks' (bm/merge tasks (:tasks b)
                           (fn [a b] (merge a b history)))]
      (flush
        (Workflow. id
                   (earliest-index start-index (:start-index b))
                   (by-earliest-index start-index (:start-index b)
                                      start-failed? (:start-failed? b))
                   (earliest-index input-index    (:input-index b))
                   (by-earliest-index input-index (:input-index b)
                                      input (:input b))
                   (earliest-index output-index    (:output-index b))
                   (by-earliest-index output-index (:output-index b)
                                      output (:output b))
                   (max last-check-index (:last-check-index b))
                   tasks'
                   (bs/union pending (:pending b))
                   errors')
        history)))

  (finalize [this history]
    (let [; Combine task errors with our own
          errors
          (->> tasks
               (mapcat (fn [entry]
                         (->> (finalize (bm/value entry) history)
                              (map (fn [err]
                                     (assoc err
                                            :task-reference-name (bm/key entry)
                                            :workflow-id id))))))
               (reduce bs/add errors))

          ; Never started
          errors
          (if (= -1 start-index)
            ; Somehow we never started?
            (bs/add errors
                    {:type        :never-started
                     :workflow-id id})
            errors)

          ; Incomplete
          errors
          (if-let [last-check (get-index history last-check-index)]
            (if (not= :completed (:status (:value last-check)))
              (let [start  (get-index history start-index)
                    start' (when start (h/completion history start))]
                (bs/add errors
                        {:type        :incomplete
                         :workflow-id id
                         :start       start
                         :start'      start'
                         :last-check' last-check}))
              errors)
            errors)

          ; Impossible output or task never executed
          errors
          (if-let [; Right now we only have one task, so its outputs are our
                   ; outputs
                   task (when (pos? (b/size tasks))
                          (bm/value (b/nth tasks 0)))]
            (if (and (not= -1 output-index)
                     (not (bs/contains? (:outputs task) output)))
              ; We learned an output that was not consistent with the task
              ; outputs
              (bs/add errors
                      {:type         :impossible-output
                       :workflow-id  id
                       :expected     (set (:outputs task))
                       :actual       output
                       :op           (h/get-index history output-index)})
              errors)
            ; Task never executed
            (bs/add errors
                    {:type         :task-never-executed
                     :workflow-id  id}))

          ; We still have pending ops--this might be a checker bug?
          errors (if (pos? (b/size pending))
                   (bs/add errors
                           {:type       :workflow-pending-ops
                            :workflow
                            (-> this
                                (update :tasks datafy)
                                (update :pending datafy)
                                (update :errors datafy)
                                (->> (into {})))})
                   errors)]
      errors)))

(defn workflow
  "Constructs a workflow for the given ID."
  [id]
  (map->Workflow {:id           id
                  :start-index  -1
                  :start-failed? false
                  :input-index  -1
                  :input        nil
                  :output-index -1
                  :output       nil
                  :last-check-index -1
                  :tasks        bm/empty
                  :pending      bs/empty
                  :errors       bs/empty}))

(defn op-workflow-id
  "Takes an invoke, a complete, and returns the workflow ID that operation was
  interacting with, or nil."
  [op op']
  (when-let [id (case (:f op)
                  :check-workflow
                  (:value op)

                  :execute
                  (:workflow-instance-id (:value op))

                  :snarf-workflow
                  (:value op)

                  :start-workflow
                  (when (and op' (h/ok? op'))
                    (:value op'))

                  nil
                  )]
    (assert (string? id)
            (str "Extracted non-string workflow ID from\n"
                 (pr-str op) "\n"
                 (pr-str op')))
    id))

(defn op-input
  "Takes an invoke, a complete, and returns a workflow input that operation
  knew about, or nil."
  [op op']
  (case (:f op)
    :check-workflow
    (:input (:value op'))

    :execute
    (:input (:value op))

    :start-workflow
    (:input (:value op))

    nil))

(defn apply-mystery-start-workflows
  "Takes a history, a map of workflow ID to Workflow, a map of inputs to IDs,
  and a map of inputs to start-workflow invocations where we don't know the ID.
  Uses the input->id map to apply as many of the mystery start-workflow
  operations as possible. Returns a pair of [workflows'
  input->start-workflows']."
  [history workflows input->id input->start-workflow]
  (loopr [workflows'              workflows
          input->start-workflow'  input->start-workflow]
         [pair input->start-workflow]
         (let [input (bm/key pair)]
           (if-let [id (bm/get input->id input)]
             ; Ah, good, we know the ID now. We can apply this to the right
             ; workflow.
             (let [w  (bm/get workflows' id ::not-found)
                   w  (if (identical? w ::not-found)
                        (workflow id)
                        w)
                   op (bm/value pair)
                   w' (step+ w history op)
                   w' (when-let [op' (h/completion history op)]
                        (step+ w' history op'))]
               (recur (bm/put workflows' id w')
                      (bm/remove input->start-workflow' input)))
             ; Dunno what this was
             (recur workflows' input->start-workflow')))))

(defn check-fold
  "A fold which checks each workflow independently, returning a map of error
  types to vectors of errors."
  [history]
  (loopf {:name "workflow"
          :associative? true}
         ; Apply each invoke and its corresponding completion, building up a
         ; map of workflow IDs to workflow states.
         ([workflows (b/linear bm/empty)
           ; A map of input to workflow IDs
           input->id (b/linear bm/empty)
           ; A map of input to start-workflow invoke ops, when we don't know
           ; what the workflow ID is. We're going to iterate over this in
           ; apply-mystery-start-workflows, and it should be infrequently used,
           ; so we use the forked variant.
           input->start-workflow bm/empty]
          [op]
          (if-not (h/invoke? op)
            ; We apply completions in the same pass as invokes
            (recur workflows input->id input->start-workflow)
            ; We have an invoke
            (let [op' (h/completion history op)
                  id  (op-workflow-id op op')
                  input (op-input op op')]
              (if (nil? id)
                (if (and (identical? :start-workflow (:f op))
                         (not (nil? input)))
                  ; We tried to start a workflow but we don't know its ID.
                  ; We'll come back to this later.
                  (recur workflows input->id
                         (bm/put input->start-workflow input op))
                  (recur workflows input->id input->start-workflow))

                ; We have a workflow ID.
                (let [; If we have an input too, remember this mapping.
                      input->id (if input
                                  (bm/put input->id input id)
                                  input->id)
                      w   (bm/get workflows id ::not-found)
                      w   (if (identical? ::not-found w)
                            (workflow id)
                            w)
                      ; Apply invocation
                      w (step+ w history op)]
                  (if (nil? op')
                    ; Done here
                    (recur (bm/put workflows id w)
                           input->id
                           input->start-workflow)
                    ; Apply completion
                    (let [w (step+ w history op')]
                      (recur (bm/put workflows id w)
                             input->id
                             input->start-workflow)))))))
          ; At the end of a chunk...
          (let [; Try to apply any start-workflow ops we weren't sure about.
                [workflows input->start-workflow]
                (apply-mystery-start-workflows
                  history workflows input->id input->start-workflow)
                ; And try to flush anything pending
                workflows
                (bm/map-values workflows (fn [k workflow]
                                          (flush workflow history)))]
            ; And seal
            [(b/forked workflows)
             (b/forked input->id)
             (b/forked input->start-workflow)]))

         ; Now merge together workflows etc.
         ([workflows              (b/linear bm/empty)
           input->id              (b/linear bm/empty)
           input->start-workflow  (b/linear bm/empty)]
          [[workflows2 input->id2 input->start-workflow2]]
          ; Just merge for now
          (recur (bm/merge workflows workflows2
                           (fn [w1 w2]
                             (merge w1 w2 history)))
                 (bm/merge input->id input->id2)
                 (bm/merge input->start-workflow input->start-workflow2))
          ; Finally
          (let [; Try to apply unknown start-workflow ops
                [workflows] (apply-mystery-start-workflows
                              history workflows input->id
                              input->start-workflow)]
            ; Flush and finalize all workloads
            (loopr [errors (b/linear bs/empty)]
                   [workflow (bm/values workflows)]
                   (recur (bs/union errors
                                    (-> workflow
                                        (flush history)
                                        (finalize history))))
                   ; And return errors grouped by type.
                   (group-by :type errors))))))

(defn workflow-errors
  "Starts a task that checks a history for errors in individual workflows."
  [history]
  (h/task history workflow-errors []
          (h/fold history (check-fold history))))

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
                (recur (bm/merge runs runs' into))))]
    (h/task history concurrent-execution-errors []
            (let [runs (h/fold history fold)]
              (->> (bm/values runs)
                   (keep (fn [intervals]
                           ; Look for an intersection
                           (loopr [a  -2
                                   a' -1]
                                  [[b b'] (sort intervals)]
                                  (let [b (long b)
                                        b' (long b')]
                                    (if (< b a') 
                                      ; Concurrent!
                                      {:type :concurrent-execution
                                       :ops  (mapv (partial h/get-index history)
                                                   (sort [a a' b b']))} 
                                      (recur b b')))
                                  nil)))
                   vec)))))

(def boring-backend-errors
  "Backend errors that sound like they might not be worth surfacing"
  [#"Connection is closed"])

(defn backend-errors
  "Takes a history. Starts a task which hunts for `:backend-error` errors,
  converting them to error maps."
  [history]
  (h/task history backend-errors []
          (->> (t/filter (fn [op]
                           (when-let [e (:error op)]
                             (when (vector? e)
                               (let [[type msg] e]
                                 (and (= :backend-error type)
                                      (not-any? #(re-find % msg)
                                                boring-backend-errors)))))))
               (t/map (fn [op']
                        {:type :backend-error
                         :op   (h/invocation history op')
                         :op'  op'}))
               (t/into [])
               (h/tesser history))))

(defn started-workflows
  "Takes a history and starts a task which returns the set of all acknowledged
  started workflow IDs."
  [history]
  (h/task history started-workflows []
          (->> (t/filter (h/has-f? :start-workflow))
               (t/filter h/ok?)
               (t/map :value)
               (t/set)
               (h/tesser history))))

(defn workflow-statuses
  "Takes a history and starts a task which returns a map of workflow ID to a
  vector of the statuses it took on, in OK order."
  [history]
  (h/task history workflow-statuses []
          (->> (t/filter (h/has-f? :check-workflow))
               (t/filter h/ok?)
               (t/map :value)
               (t/group-by :id)
               (t/map :status)
               (t/into [])
               (h/tesser history))))


(defn workflow-outputs
  "Takes a history and launches a task which returns a map of workflow ID to
  the set of all outputs it produced once completed."
  [history]
  (h/task history workflow-outputs []
          (->> (t/filter (h/has-f? :check-workflow))
               (t/filter h/ok?)
               (t/map :value)
               (t/group-by :id)
               (t/filter (comp #{:completed} :status))
               (t/map :output)
               t/set
               (h/tesser history))))

(defn last-queue-size
  "Takes a history and starts a task which returns the last successful
  :queue-size value."
  [history]
  (h/task history last-queue-size []
          (->> (t/filter (h/has-f? :queue-size))
               (t/filter h/ok?)
               (t/extremum (fn [a b] (compare (:index a) (:index b))))
               (t/post-combine :value)
               (h/tesser history))))

(defn incompatible-status-errors
  "Takes a history and a task for a map of workflow IDs to vectors of statuses.
  Starts a task which returns a collection of error maps for any workflow which
  had statuses other than :running and :complete. Later, if we add
  pauses/termination, we might relax this--I'm just trying to get a feel for
  things right now."
  [history statuses]
  (h/task history incompatible-status-errors [statuses statuses]
          (->> statuses
               (keep (fn [[id statuses]]
                       (when (not-every? #{:running :completed} statuses)
                         {:type :incompatible-status
                          :id   id
                          :statuses statuses})))
               vec)))

(defn final-status-counts
  "Takes a history and a task for a map of workflow ID to vectors of statuses.
  Starts a task which returns a frequency map of the final status for each
  workflow ID."
  [history statuses]
  (h/task history final-status-count [statuses statuses]
          (->> (vals statuses)
               (map peek)
               frequencies)))

(def normal-errors
  "These error types are considered normal; while we include them in the
  output, they don't cause us to return `:valid? false`."
  #{; Concurrent execution is normal; now that we've demonstrated it in healthy
    ; clusters, no need to hunt for it any more.
    :concurrent-execution})

(defn checker-results
  "Takes a test and a map of error types to sequences of errors, and produces a
  checker results map for it."
  [test errors]
  (let [error-types (->> errors
                         (filter (comp seq val))
                         (map key)
                         sort)
        bad-types (into (sorted-set)
                        (if-let [t (:target-error-types test)]
                          (filter t error-types)
                          (remove normal-errors error-types)))
        normal-types (->> error-types
                          (remove bad-types)
                          (into (sorted-set)))]
    (reduce (fn [res type]
              (let [errors (get errors type)]
                (assoc res type {:count    (count errors)
                                 :examples (take 16 errors)})))
            {:valid?       (not (seq bad-types))
             :normal-types normal-types
             :error-types  bad-types}
            error-types)))

(defn find-workflow-ids
  "Takes a structure and walks it looking for :workflow-id keys. Returns a set
  of those ids."
  [x]
  (let [found (volatile! (transient #{}))]
    (walk/postwalk (fn hunt [x]
                     (when (and (instance? clojure.lang.MapEntry x)
                                (identical? :workflow-id (key x)))
                       (vswap! found conj! (val x)))
                     x)
                   x)
    (persistent! @found)))

(defn trim-workflows-json!
  "We download every workflow JSON by default, since we don't know what will be
  interesting until the analysis completes, and by then the DB is gone. This
  takes a structure with :workflow-id \"...\" pairs in it, and deletes any
  *other* workflow/<id>.json files."
  [test results]
  (let [interesting (find-workflow-ids results)]
    (doseq [file (file-seq (store/path test "workflows"))]
      (when-let [[_ id] (re-find #"^(.+)\.json$" (.getName file))]
        (when-not (contains? interesting id)
          (.delete file))))))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    (h/ensure-pair-index history)
    (let [backend-errors              (backend-errors history)
          concurrent-execution-errors (concurrent-execution-errors history)
          nil-workflow-id-errors      (nil-workflow-id-errors history)
          started-workflows           (started-workflows history)
          workflow-statuses           (workflow-statuses history)
          last-queue-size             (last-queue-size history)
          workflow-outputs            (workflow-outputs history)
          incompatible-status-errors  (incompatible-status-errors
                                        history workflow-statuses)
          final-status-counts         (final-status-counts
                                        history workflow-statuses)
          workflow-errors             (workflow-errors history)
          res
          (assoc
            (checker-results
              test
              (merge-with into @workflow-errors
                          {:backend-error        @backend-errors
                           :concurrent-execution @concurrent-execution-errors
                           :incompatible-status  @incompatible-status-errors
                           :nil-workflow-id      @nil-workflow-id-errors}))
            :last-queue-size     @last-queue-size
            :started-count       (count @started-workflows)
            :final-status-counts @final-status-counts)

          ; If the last queue size wasn't zero, all bets are off.
          res (if (and @last-queue-size (pos? @last-queue-size))
                (assoc res :valid? :unknown)
                res)]
      ; As a side effect, trim unneeded JSON workflow files.
      (trim-workflows-json! test res)
      res)))

(defn checker
  "This checker detects several types of errors

      :backend-error        Some operation threw a backend error.

      :concurrent-execution For a single workflow instance and task reference
                            name, two workers executed that task concurrently.

      :different-task-input       A single task had two different inputs

      :different-workflow-input   A workflow execution had two different
                                  inputs.

      :different-workflow-output    A workflow execution had two different
                                    outputs in the completed state.

      :duplicate-workflow-id  Two create-workflow requests were assigned the
                              same ID.

      :g1a                  Aborted read. A start-workflow request returned what
                            looked like a definite failure, but actually
                            started a workflow.

      :impossible-output    A workflow produced an output that was consistent
                            with none of its task executions.

      :incomplete           A started workflow execution was not complete at
                            the end of the test.

      :incompatible-status  A workflow execution had a status we did not
                            expect.

      :missing              A read of a workflow found it missing, after we
                            know it definitely should have existed.

      :never-started        A workflow came into existence with no
                            corresponding start-workflow request.

      :nil-workflow-id      A start-workflow request returned nil, instead of
                            an ID string.

      :task-never-executed  A workflow was started but no worker was ever asked
                            to execute some task for it."
  []
  (Checker.))

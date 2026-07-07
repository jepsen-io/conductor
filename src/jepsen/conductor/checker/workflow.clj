(ns jepsen.conductor.checker.workflow
  "This namespace checks the execution of a single workflow."
  (:refer-clojure :exclude [merge flush])
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen.history :as h]
            [jepsen.history.fold :refer [loopf]]
            [potemkin :refer [definterface+]])
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
                (Task. input-index
                       input
                       outputs
                       (bs/add errors
                               {:type      :different-task-input
                                :expected input
                                :actual   (:input value)
                                :op1      (h/get-index history input-index)
                                :op2      op}))

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

(defrecord Workflow [; The workflow run id
                     id
                     ; The index of the start-workflow invocation op
                     ^long input-index
                     ; The input to the workflow
                     input
                     ; The index of the op that first told us the output
                     ^long output-index
                     ; The output of the workflow
                     output
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
                 :input-index (.index op)
                 :input (:input (.value op)))
          ; The only way we can get here is if we started the same workflow ID
          ; twice!
          (assoc this
                 :errors (bs/add errors
                                 {:type :duplicate-workflow-id
                                  :workflow-id id
                                  :op1 (h/get-index history input-index)
                                  :op2 op})))
        this)

      :check-workflow
      (let [value (.value op)]
        (cond ; Not an OK op? Nothing to learn.
              (not (h/ok? op))
              this

              ; Not-found? Also nothing to learn
              (identical? ::not-found (:status value))
              this

              ; We haven't seen the input yet; come back later
              (= -1 input-index)
              ::pending

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
                                :op2         op}))]
                (Workflow.
                  id
                  input-index'
                  input'
                  output-index'
                  output'
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
              task'     (step task history op)]
            (assoc this :tasks (bm/put tasks ref-name task'))))

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
                   (earliest-index input-index    (:input-index b))
                   (by-earliest-index input-index (:input-index b)
                                      input (:input b))
                   (earliest-index output-index    (:output-index b))
                   (by-earliest-index output-index (:output-index b)
                                      output (:output b))
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

          ; No task ever even started executing?
          (bs/add errors
                  {:type         :task-never-executed
                   :workflow-id  id}))
          errors (if (pos? (b/size pending))
                   ; We have pending ops still
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
                  :input-index  -1
                  :input        nil
                  :output-index -1
                  :output       nil
                  :tasks        bm/empty
                  :pending      bs/empty
                  :errors       bs/empty}))

(defn op-workflow-id
  "Takes an invoke, a complete, and returns the workflow ID that operation was
  interacting with, or nil."
  [op op']
  (case (:f op)
    :check-workflow
    (:value op)

    :execute
    (:workflow-instance-id (:value op))

    :start-workflow
    (:value op')

    :queue-size
    nil))

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

    :queue-size
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
                   w' (step+ w history op)]
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
           ; what the workflow ID is.
           input->start-workflow (b/linear bm/empty)]
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

(defn errors
  "Starts a task that checks a history for errors."
  [history]
  (h/task history workflow-errors []
          (h/fold history (check-fold history))))

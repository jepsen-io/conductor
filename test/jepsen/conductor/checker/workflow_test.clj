(ns jepsen.conductor.checker.workflow-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [checker :as checker]
                    [history :as h]
                    [util :as util]]
            [jepsen.conductor.checker.workflow :as c]))

(defn check
  [history]
  (checker/check (c/checker) {:start-time (util/local-time)
                              :name "workflow-test"} history {}))

(deftest backend-error-test
  (let [h (h/history
            [{:index 21, :time 379337896, :type :invoke, :process 21, :f :start-workflow, :value {:name "greetings", :version 1, :input {"name" "jepsen 19"}}}
             {:index 32, :time 517876377, :type :info, :process 21, :f :start-workflow, :value {:name "greetings", :version 1, :input {"name" "jepsen 19"}}, :error [:backend-error "BACKEND_ERROR - ERROR: syntax error at or near \"null\"\n  Position: 13"]}])]
    (is (= {:count 1
            :examples [{:type :backend-error
                        :op (h 0)
                        :op' (h 1)}]}
           (:backend-error (check h))))))


(deftest concurrent-execution-errors-test
  (let [h (h/history
            [{:process 0, :type :invoke, :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}
             ; Note that this :info counts as "the end of execution" for our purposes.
             {:process 0, :type :info,   :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}

             ; Now start it again
             {:process 1, :type :invoke, :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}

             ; This is fine; different task
             {:process 2, :type :invoke, :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t2"}}
             {:process 2, :type :ok,     :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t2"}}
             ; Also fine; different workflow
             {:process 3, :type :invoke, :f :execute, :value {:workflow-instance-id "w2", :reference-task-name "t"}}
             {:process 3, :type :ok,     :f :execute, :value {:workflow-instance-id "w2", :reference-task-name "t"}}

             ; But this, oh, that's a violation
             {:process 4, :type :invoke, :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}
             {:process 4, :type :ok,     :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}

             ; Complete
             {:process 1, :type :ok,     :f :execute, :value {:workflow-instance-id "w", :reference-task-name "t"}}])
        ]
    (is (= {:count 1
            :examples [{:type :concurrent-execution
                        ; These two ops were concurrently executing the same workflow instance and reference task name
                        :ops (mapv h [2 7 8 9])}]}
           (:concurrent-execution (check h)))))

  (testing "ignores sequential ops"
    (let [h (h/history
              [{:index 0,
                :time 122966562902,
                :type :invoke,
                :process :n10.0.105,
                :f :execute,
                :value {:reference-task-name "greet_ref",
                        :workflow-instance-id "3004ec07-7663-11f1-8270-00163e983f22",
                        :task-id "30053a2a-7663-11f1-8270-00163e983f22"}}
               {:index 1,
                :time 122967398856,
                :type :ok,
                :process :n10.0.105,
                :f :execute,
                :value nil}
               {:index 2,
                :time 131344481896,
                :type :invoke,
                :process :n8.0.1,
                :f :execute,
                :value {:reference-task-name "greet_ref",
                        :workflow-instance-id "3004ec07-7663-11f1-8270-00163e983f22",
                        :task-id "41e94115-7663-11f1-8269-00163eb95b8c"}}
               {:index 3,
                :time 131345109627,
                :type :ok,
                :process :n8.0.1,
                :f :execute,
                :value nil}])]
      (is (= nil
             (:concurrent-execution (check h)))))))

(deftest task-never-executed-test
  (let [h (h/history
            [{:process 0, :type :invoke, :f :start-workflow}
             {:process 0, :type :ok, :f :start-workflow, :value "a"}])]
    (is (= {:count 1
            :examples [{:type        :task-never-executed
                        :workflow-id "a"}]}
           (:task-never-executed (check h))))))

(deftest abandoned-test
  (let [h (h/history
            [; Start two workflows
             {:process 0, :type :invoke, :f :start-workflow}
             {:process 0, :type :ok, :f :start-workflow, :value "a"}
             {:process 0, :type :invoke, :f :start-workflow}
             {:process 0, :type :ok, :f :start-workflow, :value "b"}
             ; Observe that both are running
             {:process 0, :type :invoke, :f :check-workflow, :value "a"}
             {:process 0, :type :ok, :f :check-workflow,
              :value {:id "a", :status :running}}
             {:process 0, :type :invoke, :f :check-workflow, :value "b"}
             {:process 0, :type :ok, :f :check-workflow,
              :value {:id "b", :status :running}}
             ; And confirm the queue only has one element
             {:process 0, :type :invoke, :f :queue-size}
             {:process 0, :type :ok, :f :queue-size, :value 1}])]
    (is (= {:valid? false,
            ; Since the final queue size was smaller than the number of pending
            ; tasks
            :abandoned {:count 1,
                        :examples
                        [{:type :abandoned,
                          :last-queue-size 1,
                          :incomplete-count 2}]},
            ; Since we never observed an execute
            :task-never-executed
            {:count 2,
             :examples [{:type :task-never-executed, :workflow-id "b"}
                        {:type :task-never-executed, :workflow-id "a"}]},
            :final-status-counts {:running 2},
            :normal-types #{},
            :started-count 2,
            ; Since the final statuses were running
            :incomplete
            {:count 2,
             :examples
             [{:type :incomplete, :id "a", :statuses [:running]}
              {:type :incomplete, :id "b", :statuses [:running]}]},
            :last-queue-size 1,
            :error-types #{:abandoned :incomplete :task-never-executed}}
           (check h)))))

(deftest different-workflow-input-test
  ; When check-workflow reports an inconsistent input with the start...
  (let [h (h/history
            [{:process 1, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 1, :type :ok, :f :check-workflow, :value {:id     "w1"
                                                                 :status :running
                                                                 :input {"in" "red"}}}
             ; Even if the start-workflow happens out of order--this is
             ; nonphysical but we want to stress the pending fsm
             {:process 0, :type :invoke, :f :start-workflow, :value {:input {"in" "blue"}}}
             {:process 0, :type :ok, :f :start-workflow, :value "w1"}])]
    (is (= {:valid? false
            :started-count 1
            :final-status-counts {:running 1}
            :normal-types #{}
            :error-types #{:incomplete :task-never-executed :different-workflow-input}
            :incomplete {:count 1
                         :examples [{:type :incomplete
                                     :id "w1"
                                     :statuses [:running]}]}
            :task-never-executed {:count 1
                                  :examples [{:type :task-never-executed
                                              :workflow-id "w1"}]}
            :different-workflow-input {:count 1
                                       :examples [{:type        :different-workflow-input
                                                   :workflow-id "w1"
                                                   :op1         (h 2)
                                                   :op2         (h 1)
                                                   :expected {"in" "blue"}
                                                   :actual   {"in" "red"}}]}
            :last-queue-size nil}
           (check h)))))

(deftest different-workflow-output-test
  ; When check-workflow reports two inconsistent outputs...
  (let [h (h/history
            [{:process 1, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 1, :type :ok, :f :check-workflow, :value {:id     "w1"
                                                                 :status :completed
                                                                 :input :x
                                                                 :output :y}}
             ; Even if the start-workflow happens out of order--this is
             ; nonphysical but we want to stress the pending fsm
             {:process 0, :type :invoke, :f :start-workflow, :value {:input :x}}
             {:process 0, :type :ok, :f :start-workflow, :value "w1"}
             {:process 2, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 2, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :status :completed
                                                                 :input :x
                                                                 ; Changed!
                                                                 :output :z}}])]
    (is (= {:valid? false
            :started-count 1
            :final-status-counts {:completed 1}
            :normal-types #{}
            :error-types #{:task-never-executed :different-workflow-output}
            :task-never-executed {:count 1
                                  :examples [{:type :task-never-executed
                                              :workflow-id "w1"}]}
            :different-workflow-output {:count 1
                                        :examples [{:type        :different-workflow-output
                                                    :workflow-id "w1"
                                                    :op1         (h 5)
                                                    :op2         (h 1)
                                                    :expected :z
                                                    :actual   :y}]}
            :last-queue-size nil}
           (check h)))))

(deftest different-task-input-test
  (testing "different start-workflow and execute input"
    (let [h (h/history
              [{:process 0, :type :invoke, :f :start-workflow, :value {:input {"in" "blue"}}}
               {:process 0, :type :ok, :f :start-workflow, :value "w1"}
               {:process 1, :type :invoke, :f :execute, :value {:reference-task-name "t1"
                                                                :workflow-instance-id "w1"
                                                                :task-id              "t"
                                                                :input                {"in" "red"}
                                                                :output               {}}}
               {:process 2, :type :invoke, :f :check-workflow, :value "w1"}
               {:process 2, :type :ok, :f :check-workflow, :value {:id "w1", :status :completed}}])]
      (is (= {:valid? false
              :started-count 1
              :last-queue-size nil
              :final-status-counts {:completed 1}
              :normal-types #{}
              :error-types #{:different-task-input}
              :different-task-input
              {:count 1
               :examples [{:type     :different-task-input
                           :op1      (h 0)
                           :op1'     (h 1)
                           :op2      (h 2)
                           :expected {"in" "blue"}
                           :actual   {"in" "red"}}]}}
             (check h))))))

(deftest impossible-output-test
  ; A workflow returns a value produced by no execution
  (let [h (h/history
            [{:process 0, :type :invoke, :f :execute, :value {:reference-task-name  "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "red"}
                                                              :output               {"out" "star"}}}
             {:process 1, :type :invoke, :f :start-workflow, :value {:input {"in" "red"}}}
             {:process 1, :type :ok, :f :start-workflow, :value "w1"}
             {:process 2, :type :invoke, :f :execute, :value {:reference-task-name "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "red"}
                                                              :output               {"out" "plain"}}}
             ; Whoah, weird output
             {:process 3, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 3, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :status :completed
                                                                 :input  {"in" "red"}
                                                                 :output {"out" "bouba"}}}])]
    (is (= {:valid? false
            :started-count 1
            :last-queue-size nil
            :final-status-counts {:completed 1}
            :normal-types #{}
            :error-types #{:impossible-output}
            :impossible-output
            {:count 1
             :examples [{:type        :impossible-output
                         :workflow-id "w1"
                         :op          (h 5)
                         :expected    #{{"out" "star"} {"out" "plain"}}
                         :actual      {"out" "bouba"}}]}}
           (check h)))))

(deftest different-task-input-test
  (let [h (h/history
            [{:process 0, :type :invoke, :f :execute, :value {:reference-task-name  "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "red"}
                                                              :output               {"out" "star"}}}
             {:process 1, :type :invoke, :f :start-workflow, :value {:input {"in" "red"}}}
             {:process 1, :type :ok, :f :start-workflow, :value "w1"}
             ; Wrong input
             {:process 2, :type :invoke, :f :execute, :value {:reference-task-name "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "blue"}
                                                              :output               {"out" "plain"}}}
             {:process 3, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 3, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :status :completed
                                                                 :input  {"in" "red"}
                                                                 :output {"out" "plain"}}}])]
    (is (= {:valid?              false
            :started-count       1
            :last-queue-size     nil
            :final-status-counts {:completed 1}
            :normal-types        #{}
            :error-types         #{:different-task-input}
            :different-task-input
            {:count 1
             :examples [{:type        :different-task-input
                         :workflow-id "w1"
                         :task-reference-name "t1"
                         :op1         (h 1)
                         :op1'        (h 2)
                         :op2         (h 3)
                         :expected    {"in" "red"}
                         :actual      {"in" "blue"}}]}}
           (check h)))))

(deftest start-workflow-info-task-input-test
  ; We want to be able to stitch together a task even if we don't get its ID
  ; back from start-workflow, based on its unique input.
  (let [h (h/history
            [; Start-workflow fails
             {:process 1, :type :invoke, :f :start-workflow, :value {:input {"in" "red"}}}
             {:process 1, :type :info, :f :start-workflow, :value nil}
             ; But we pick it up from this task input
             {:process 0, :type :invoke, :f :execute, :value {:reference-task-name  "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "red"}
                                                              :output               {"out" "star"}}}
             ; And when we check it, we find the wrong output
             {:process 3, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 3, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :status :completed
                                                                 :input  {"in" "red"}
                                                                 :output {"out" "plain"}}}])]
    (is (= {:valid? false
            :started-count 0 ; We're not smart enough to know this started
            :last-queue-size nil
            :final-status-counts {:completed 1}
            :normal-types #{}
            :error-types #{:impossible-output}
            :impossible-output
            {:count 1
             :examples [{:type        :impossible-output
                         :workflow-id "w1"
                         :op          (h 4)
                         :expected    #{{"out" "star"}}
                         :actual      {"out" "plain"}}]}}
           (check h)))))

(deftest duplicate-workflow-id-test
  (let [h (h/history
            [{:process 1, :type :invoke, :f :start-workflow, :value {:input {"in" "red"}}}
             {:process 1, :type :ok,     :f :start-workflow, :value "w1"}
             {:process 2, :type :invoke, :f :start-workflow, :value {:input {"in" "blue"}}}
             {:process 2, :type :ok,     :f :start-workflow, :value "w1"}])]
    (is (= {:count 1
            :examples [{:type :duplicate-workflow-id
                        :workflow-id "w1"
                        :op1         (h 0)
                        :op1'        (h 1)
                        :op2         (h 2)
                        :op2'        (h 3)}]}
           (:duplicate-workflow-id (check h))))))

(deftest never-started-test
  (let [h (h/history
            [; A read from nothing
             {:process 2, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 2, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :input {"in" "red"}
                                                                 :status :running}}])]
    (is (= {:count 1
            :examples [{:type         :never-started
                        :workflow-id  "w1"}]}
           (:never-started (check h))))))

(deftest g1a-test
  (let [h (h/history
            [; Start-workflow fails
             {:process 1, :type :invoke, :f :start-workflow, :value {:input {"in" "red"}}}
             {:process 1, :type :fail, :f :start-workflow, :value nil, :error :oh-no}
             ; But we read it
             {:process 2, :type :invoke, :f :check-workflow, :value "w1"}
             {:process 2, :type :ok, :f :check-workflow, :value {:id "w1"
                                                                 :input {"in" "red"}
                                                                 :status :running}}
             ; Or executed it
             {:process 3, :type :invoke, :f :execute, :value {:reference-task-name "t1"
                                                              :workflow-instance-id "w1"
                                                              :task-id              "t"
                                                              :input                {"in" "red"}
                                                              :output               {"out" "plain"}}}])]
    (is (= {:count 2
            :examples [{:type        :g1a
                        :workflow-id "w1"
                        :start       (h 0)
                        :start'      (h 1)
                        :op          (h 3)}
                       {:type        :g1a
                        :workflow-id "w1"
                        :start       (h 0)
                        :start'      (h 1)
                        :op          (h 4)}]}
           (:g1a (check h))))))

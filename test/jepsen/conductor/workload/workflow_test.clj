(ns jepsen.conductor.workload.workflow-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.conductor.workload.workflow :as w]))

(deftest backpressure-interval-test
  (let [int-10khz 100000]
    ; If the queue is steady and empty, we'll try to speed up a little
    (is (= 100000 (w/backpressure-interval 0 0 int-10khz)))
    ; If, on the other hand, it's stuck at a thousand elements, we should slow down.
    (is (= 100794 (w/backpressure-interval 1000 0 int-10khz)))
    ; If we stay there, it'll converge quick
    (is (= 101601 (w/backpressure-interval 1000 0 100794)))
    (is (= 102421 (w/backpressure-interval 1000 0 101601)))

    ; If we start rising REALLY fast, it'll back off to the min rate
    (is (= 875012500 (w/backpressure-interval 1000 1 int-10khz)))

    ; If we end up with 10K elements in queue steady, it'll slow rather than speed up
    (is (= 109614 (w/backpressure-interval 10000 0 int-10khz)))
    ))

(defn check
  [history]
  (checker/check (w/completion-checker) {} history {}))

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

(deftest task-never-started-test
  (let [h (h/history
            [{:process 0, :type :invoke, :f :start-workflow}
             {:process 0, :type :ok, :f :start-workflow, :value "a"}])]
    (is (= {:count 1
            :examples [{:type        :task-never-started
                        :workflow-id "a"}]}
           (:task-never-started (check h))))))

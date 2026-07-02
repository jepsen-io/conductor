(ns jepsen.conductor.workload.workflow-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
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
    (is (= [{:type :concurrent-execution
             ; These two ops were concurrently executing the same workflow instance and reference task name
             :ops (mapv h [2 7 8 9])}]
           @(w/concurrent-execution-errors h)))))

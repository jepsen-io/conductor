(ns jepsen.conductor.workload.workflow-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
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

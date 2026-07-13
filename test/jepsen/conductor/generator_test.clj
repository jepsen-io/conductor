(ns jepsen.conductor.generator-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
            [jepsen.conductor.generator :as g]))

(deftest backpressure-interval-test
  (let [int-10khz 100000]
    ; Remmeber, larger intervals are *slower* request rates!

    ; If the queue is steady and empty, we'll try to speed up a little
    (is (= 100000 (g/backpressure-interval 0 0 int-10khz)))
    ; If, on the other hand, it's stuck at 10,0000 elements, we should slow down.
    (is (= 108653 (g/backpressure-interval 10000 0 int-10khz)))
    ; If we stay there, it'll converge quick
    (is (= 108653 (g/backpressure-interval 1000 0 108653)))

    ; If we start rising REALLY fast, it'll back off to the min rate
    (is (= 875012500 (g/backpressure-interval 1000 1 int-10khz)))

    ; If we end up with 10K elements in queue steady, it'll slow rather than speed up
    (is (= 108653 (g/backpressure-interval 10000 0 int-10khz)))
    ))

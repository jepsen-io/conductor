(ns jepsen.conductor.checker.plots
  "Various plots"
  (:require [bifurcan-clj [core :as b]
                          [int-map :as bim]
                          [map :as bm]]
            [clj-commons.slingshot :refer [try+]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [jepsen [checker :as checker]
                    [history :as h]
                    [store :as store]
                    [util :refer [nanos->secs]]]
            [jepsen.checker [perf :as perf]]
            [jepsen.history.fold :refer [loopf]]
            [tesser.core :as t]))

(defn time-series!
  "Renders a simple time-series plot. Options:

    :filename   The filename to render
    :title      The title of the plot
    :ylabel     The y axis label
    :series     A collection of series maps; see perf/plot! for details."
  [test history checker-opts {:keys [filename title ylabel series]}]
  (let [output (.getCanonicalPath
                 (store/path! test (:subdirectory checker-opts) filename))
        nemeses (:nemeses checker-opts (:nemeses (:plot test)))
        preamble (concat (perf/preamble output)
                         [['set 'title title
                           'set 'ylabel ylabel]])]
    (-> {:preamble preamble, :series series}
        perf/with-range
        (perf/with-nemeses history nemeses)
        perf/plot!
        (try+ (catch [:type :jepsen.checker.perf/no-points] _
                :jepsen.checker.perf/no-points)))))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    ; Queue size
    (time-series!
      test history opts
      {:title    "Decider queue size"
       :ylabel   "Elements"
       :filename "queue-size.png"
       :series   [{:title "Queue"
                   :with 'lines
                   :data  (->> (t/filter (h/has-f? :queue-size))
                               (t/filter h/ok?)
                               (t/map (juxt (comp nanos->secs :time) :value))
                               (t/into [])
                               (h/tesser history))}]})

    ; Workflow rate
    (time-series!
      test history opts
      {:title "start-workflow rate"
       :ylabel "Workflows / s"
       :filename "workflow-rate.png"
       :series [{:title "Rate"
                 :with 'lines
                 :data (h/fold
                         history
                         (loopf
                           {:name "start-workflow rate"}
                           ([buckets (b/linear (bim/int-map))]
                            [op]
                            (recur
                              (if (and (h/invoke? op)
                                       (identical? :start-workflow (:f op)))
                                (bim/update buckets
                                            ; One per second
                                            (quot (:time op) 1000000000)
                                            (fn [c]
                                              (if c (inc c) 1)))
                                buckets))
                            (b/linear buckets))
                           ; Merge buckets
                           ([buckets (b/linear (bim/int-map))]
                            [buckets']
                            (recur (bm/merge buckets buckets' +))
                            (map datafy buckets))))}]})


      {:valid? true}))

(defn checker
  "A checker that renders a plot of queue size over time."
  []
  (Checker.))

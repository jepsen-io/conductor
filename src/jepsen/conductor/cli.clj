(ns jepsen.conductor.cli
  "Command-line entry point for Conductor tests"
  (:gen-class)
  (:require [clojure [math :as math]
                     [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as control]
                    [generator :as gen]
                    [nemesis :as jepsen.nemesis]
                    [os :as os]
                    [random :as rand]
                    [tests :as tests]
                    [util :as util :refer [default]]]
            [jepsen.conductor [db :as db]
                              [nemesis :as nemesis]]
            [jepsen.conductor.workload [workflow :as workflow]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none                 (constantly tests/noop-test)
   :workflow             workflow/workload})

(def standard-workloads
  "All the workloads we run by default."
  [:workflow])

(def nemeses
  "Basic nemeses we have available."
  #{:kill
    :kill-db
    :kill-queue
    :kill-server
    :kill-std-worker
    :kill-worker
    :partition
    :partition-server
    :pause
    :pause-db
    :pause-queue
    :pause-server
    :pause-std-worker
    :pause-worker
    :packet
    :clock
    :clock-db
    :clock-queue
    :clock-server
    :clock-std-worker
    :clock-worker
    })

(def file-corruption-nemeses
  "Which nemeses corrupt files?"
  #{:bitflip-file-chunks
    :snapshot-file-chunks
    :truncate-file})

(def db-node-targets
  "Different ways we can target single nodes for database faults."
  #{:one
    :minority
    :majority
    :all})

(def standard-nemeses
  "Combinations of nemeses we run by default."
  [; Nothing
   []
   ; One fault at a time
   [:partition]
   [:kill]
   [:pause]
   [:clock]
   [:packet]
   ; General chaos
   [:partition :pause :kill :clock
    ;:bitflip-file-chunks :snapshot-file-chunks :truncate-file-chunks
    ]
   ])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all (peek standard-nemeses)})

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (mapv keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(defn test-name
  "Takes CLI options and constructs a test name as a string."
  [opts]
  (str (str/replace (:version opts) "/" "-") " "
       (name (:workload opts))
       (let [n (:nemesis opts)]
         (when (seq n)
           (str " " (->> n (map name) sort (str/join ",")))))))

(defn roles
  "The Jepsen role map, given CLI options."
  [opts]
  (let [[db queue & more] (:nodes opts)
        n (count more)
        _ (assert (< 2 n) "Need at least five nodes to run a Conductor cluster.")
        more (vec more)
        c1   (math/round (* 1/3 n))
        c2   (math/round (* 2/3 n))]
    {:db            [db]
     :queue         [queue]
     :server        (subvec more 0 c1)
     :std-worker    (subvec more c1 c2)
     :worker        (subvec more c2)}))

(defn conductor-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name   (:workload opts)
        workload        ((workloads workload-name) opts)
        opts            (assoc opts :roles (roles opts))
        db              (db/db opts)
        os              debian/os
        nemesis         (nemesis/package
                          {:db            db
                           :roles         (:roles opts)
                           :nodes         (:nodes opts)
                           :stable-period (:nemesis-stable-period opts)
                           :interval      (:nemesis-interval opts)
                           :faults        (:nemesis opts)
                           :partition     {:targets [:one :majority]}
                           :pause         {:targets (:db-node-targets opts)}
                           :kill          {:targets (:db-node-targets opts)}
                           :packet        {:targets [:one :minority :all]
                                           :behaviors
                                           [{:delay {:time   "100ms"
                                                     :jitter "50ms"}}]
                                           }})
        workload-gen (->> (:generator workload)
                          (gen/stagger (/ (:rate opts)))
                          gen/clients)
        nemesis-gen (->> (:generator nemesis)
                         gen/nemesis)
        ; Main phase generator
        gen (->> workload-gen
                   (gen/nemesis
                     (gen/phases
                       (gen/sleep (:initial-quiet-period opts))
                       (:generator nemesis))))
        ; Limit
        gen (gen/time-limit (:time-limit opts) gen)
        ; And afterwards, recover and final gen
        gen (gen/phases
              gen
              ; Recover
              (gen/nemesis
                [(gen/log "Recovering")
                 (:final-generator nemesis)
                 (gen/sleep (:recovery-time opts))
                 (gen/log "Beginning final generator")])
              ; Final gen
              (->> (:final-generator workload)
                   (gen/time-limit (:final-time-limit opts))
                   gen/clients))
        ; And wrap
        gen (if-let [wrap (:wrap-generator workload)]
              (wrap gen)
              gen)]
    (-> tests/noop-test
        (merge
          opts
          {:name     (test-name opts)
           :os       os
           :db       db
           :checker  (checker/compose
                       {:perf       (checker/perf)
                        :clock      (checker/clock-plot)
                        :stats      (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        :workload   (:checker workload)})
           :client    (:client workload)
           :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
           :plot      {:nemeses (:perf nemesis)}
           :generator gen
           ;:logging {:overrides
           ;          {"..." "FATAL"}}
           }))))

(def cli-opts
  "Command-line option specification"
  (cli/merge-opt-specs
    []
    [[nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
      :default  "3n"
      :validate [(partial re-find #"^\d+n?$")
                 "Must be an integer, optionally followed by n."]]

     [nil "--db-node-targets TARGETS" "A comma-separated list of ways to target DB nodes for faults, like 'one,majority'"
      :default  [:one :majority :all]
      :parse-fn parse-comma-kws
      :validate [(partial every? db-node-targets) (cli/one-of db-node-targets)]]

     [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
      :default  300
      :parse-fn read-string
      :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

     [nil "--initial-quiet-period SECONDS" "How long to wait before beginning faults"
      :default  10
      :parse-fn read-string
      :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

     [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
      :default  #{}
      :parse-fn parse-nemesis-spec
      :validate [(partial every? (into nemeses (keys special-nemeses)))
                 (str (cli/one-of nemeses) " or the special nemeses, which " (cli/one-of special-nemeses))]]

     [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
      :default  5
      :parse-fn read-string
      :validate [pos? "Must be a positive number."]]

     [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
      :default  30
      :parse-fn parse-long
      :validate [pos? "Must be a positive number."]]

     ["-r" "--rate HZ" "Approximate request rate, in hz"
      :default  10000
      :parse-fn read-string
      :validate [pos? "Must be a positive number."]]

     [nil "--recovery-time SECONDS" "How long to wait after the end of fault injection before beginning the final generator."
      :default 60
      :parse-fn parse-long
      :validate [(complement neg?) "Must not be negative"]]

     [nil "--time-limit SECONDS"
      "Excluding setup and teardown, how long should a test run for, in seconds?"
      :default  300
      :parse-fn #(Long/parseLong %)
      :validate [pos? "Must be positive"]]

     ["-v" "--version VERSION" "What version should we test? Should be a path to a directory containing lib/worker.jar and lib/server.jar on the control node."
      :default "versions/5.3.2"]

     ["-w" "--workload NAME" "What workload should we run?"
      :default  :workflow
      :parse-fn keyword
      :validate [workloads (cli/one-of workloads)]]
     ]))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads (if-let [w (:workload opts)] [w] standard-workloads)]
    (for [i     (range (:test-count opts))
          n     nemeses
          w     workloads]
      (conductor-test (-> opts
                          (assoc :nemesis  n
                                 :workload w))))))
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  conductor-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd
                     {:tests-fn all-tests
                      :opt-spec (cli/without-defaults-for
                                  [:workload :nemesis]
                                  cli-opts)})
                   (cli/serve-cmd))
            args))

(ns jepsen.conductor.cli
  "Command-line entry point for Conductor tests"
  (:gen-class)
  (:require [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [math :as math]
                     [pprint :refer [pprint]]
                     [set :as set]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as control]
                    [generator :as gen]
                    [nemesis :as jepsen.nemesis]
                    [os :as os]
                    [random :as rand]
                    [tests :as tests]
                    [util :as util :refer [default]]]
            [jepsen.conductor [core :as core]
                              [db :as db]
                              [nemesis :as nemesis]]
            [jepsen.conductor.checker.plots :as plots]
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

(def db-node-targets
  "Different ways we can target single nodes for database faults."
  #{:one
    :minority
    :majority
    :all})

(def nemeses
  "Basic nemeses we have available."
  #{:kill
    :kill-postgres
    :kill-redis
    :kill-server
    :kill-std-worker
    :kill-worker
    :partition
    :partition-server
    :partition-server-postgres
    :partition-server-redis
    :partition-server-std-worker
    :partition-server-worker
    :pause
    :pause-postgres
    :pause-redis
    :pause-server
    :pause-std-worker
    :pause-worker
    :packet
    :clock
    :clock-postgres
    :clock-redis
    :clock-server
    :clock-std-worker
    :clock-worker
    })

(def standard-nemeses
  "Combinations of nemeses we run by default."
  (vec
    (concat ; Nothing
            [[]]
            ; Each of the single nemeses above
            (->> nemeses sort (map vector))
            ; General chaos
            [[:partition :pause :kill :packet :clock]])))

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

(defn cut-vec
  "Cuts a vector into a vector of disjoint subvectors, with each subvector's
  count given by the corresponding value in `counts`."
  [v counts]
  (loopr [i 0
          chunks []]
         [c counts]
         (recur (+ i c)
                (conj chunks (subvec v i (+ i c))))
         chunks))

(defn roles
  "The Jepsen role map, given CLI options."
  [{:keys [nodes redis-type]}]
  (let [n     (count nodes)
        pg    1
        redis (case redis-type
                :single 1
                ; The only database I have ever seen that needs a minimum of
                ; six nodes and *still* loses your data by design
                :cluster 6)
        ; Divide the remaining nodes equally
        m          (- n pg redis)
        _          (when-not (<= 3 m)
                     (throw+ {:type ::not-enough-nodes
                              :have (count nodes)
                              :need (+ pg redis 3)}))
        server     (math/floor (* 1/3 m))
        std-worker (math/floor (* 1/3 m))
        worker     (- m server std-worker)]
      (zipmap [:postgres :redis :server :std-worker :worker]
              (cut-vec nodes [pg redis server std-worker worker]))))

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
              (gen/log "Recovering")
              (gen/nemesis (:final-generator nemesis))
              ; While it's recovering...
              (->> (:recovery-generator workload)
                   (gen/stagger (/ (:recovery-rate opts)))
                   (gen/clients)
                   (gen/time-limit (:recovery-time-limit opts)))
              ; Final gen
              (gen/log "Beginning final generator")
              (->> (:final-generator workload)
                   (gen/clients)
                   (gen/time-limit (:final-time-limit opts))))
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
                        :queue-size (plots/checker)
                        :workload   (:checker workload)})
           :client    (:client workload)
           :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
           :plot      {:nemeses (:perf nemesis)}
           :generator gen
           :logging {:overrides
                     ; The Orkes client will complain about not having a key or
                     ; secret... often
                     {"io.orkes.conductor.client.ApiClient" "ERROR"}}
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

     [nil "--db-type TYPE" "Conductor's db.type setting. Either redis or postgres"
      :default :redis
      :parse-fn keyword
      :validate [#{:postgres :redis} "Must be either redis or postgres."]]

     [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
      :default  600
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

     [nil "--queue-type TYPE" "Conductor's queue.type setting. Either redis or postgres"
      :default :redis
      :parse-fn keyword
      :validate [#{:postgres :redis} "Must be either redis or postgres."]]

     ["-r" "--rate HZ" "Approximate request rate, in hz"
      :default  10000
      :parse-fn read-string
      :validate [pos? "Must be a positive number."]]

     [nil "--recovery-rate HZ" "How fast should we run operations during the recovery period? We intentionally choose a lower value here to avoid overloading the system during recovery."
      :default 100
      :parse-fn read-string
      :validate [pos? "Must be a positive number."]]

     [nil "--recovery-time-limit SECONDS" "How long to wait after the end of fault injection before beginning the final generator."
      :default 600
      :parse-fn parse-long
      :validate [(complement neg?) "Must not be negative"]]

     [nil "--redis-type TYPE" "Either `single` for a single node, or `cluster` for Redis Cluster."
      :default :single
      :parse-fn keyword
      :validate [#{:single :cluster} "Must be either `single` or `cluster`."]]

     [nil "--response-timeout SECONDS" "How long before Conductor considers one of our tasks timed out, and can retry it? You probably want this to be shorter than the recovery period, so that Conductor has a chance to detect and re-schedule tasks that got stuck during the test run."
      :default 10
      :parse-fn parse-long
      :validate [pos? "Must be positive"]]

     [nil "--target-error-types TYPES" "Hunts for any of a comma-separated list of error types from `workload.workflow/completion-checker`, e.g. `incomplete,task-never-executed`. Other errors are reported, but do not contribute to a failing result."
      :parse-fn (comp set parse-comma-kws)]

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
  (with-redefs [jepsen.core/run! core/run-test!]
    (cli/run! (merge (cli/single-test-cmd {:test-fn  conductor-test
                                           :opt-spec cli-opts})
                     (cli/test-all-cmd
                       {:tests-fn all-tests
                        :opt-spec (cli/without-defaults-for
                                    [:workload :nemesis]
                                    cli-opts)})
                     (cli/serve-cmd))
              args)))

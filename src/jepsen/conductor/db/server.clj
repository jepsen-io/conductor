(ns jepsen.conductor.db.server
  "Database automation for the Conductor server."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java [io :as io]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [fs-cache :as fs-cache]
                    [random :as rand]
                    [role :as role]
                    [util :refer [meh]]]
            [jepsen.conductor.base :refer :all]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]))

(def dir
  "Where do we install the server remotely?"
  "/opt/conductor-server")

(def jar-file
  "Where do we put the jar file?"
  (str dir "/server.jar"))

(def properties-file
  "Where do we write worker.properties?"
  (str dir "/server.properties"))

(def log-file
  "Where do we log daemon output?"
  (str dir "/server.log"))

(def pid-file
  "Where do we store the pidfile?"
  (str dir "/server.pid"))

(def version-file
  "Where do we stash the version string?"
  (str dir "/version"))

(defn install!
  "Installs the server."
  [test node]
  (let [version (:version test)]
    (c/su
      (debian/install [:openjdk-25-jdk-headless :ansifilter])
      (when-not (and (cu/file? version-file)
                     (= version (c/exec :cat version-file)))
        (c/su (c/exec :mkdir :-p dir))
        (info "Uploading server")
        (c/upload (str (:version test) "/lib/server.jar")
                  jar-file)
        (cu/write-file! version version-file)
        (info "Uploaded server")))))

(defn configure!
  "Writes out the properties file."
  [test node]
  (c/su
    (write-properties!
      {; Postgres
       "spring.datasource.url" (str "jdbc:postgresql://"
                                    (-> test :roles :postgres rand/nth)
                                    ":5432/")
       ; These are the credentials jepsen.postgres provisions by default
       "spring.datasource.username" "postgres"
       "spring.datasource.password" "pw"
       "spring.datasource.hikari.maximum-pool-size" 64

       ; Redis
       "conductor.redis.hosts" (->> (:roles test)
                                    :redis
                                    (mapv (fn [node]
                                            ; We'll pretend each node is in its
                                            ; own rack
                                            (str node ":6379:" node ":pw")))
                                    (str/join ","))
       "conductor.redis.username"                 "conductor"
       "conductor.redis.password"                 "pw"
       "conductor.redis.workflowNamespacePrefix"  "conductor"
       "conductor.redis.queueNamespacePrefix"     "conductor_queues"
       "conductor.redis.queuesNonQuorumPort"      6379

       "conductor.db.type"    (case (:db-type test)
                                :postgres "postgres"
                                :redis (case (:redis-type test)
                                         :single "redis_standalone"
                                         :cluster "redis_cluster"))
       "conductor.queue.type" (case (:queue-type test)
                                :postgres "postgres"
                                :redis (case (:redis-type test)
                                         :single "redis_standalone"
                                         :cluster "redis_cluster"))

       ; And the archive/index store is Postgres
       "conductor.archive.db.enabled" true
       "conductor.archive.db.type" "postgres"
       "conductor.archive.db.document.store.type" "postgres"

       ; Don't do async indexing of executions into Postgres
       "conductor.app.asyncIndexingEnabled" false

       ; Disable auth
       "conductor.security.enabled" "false"

       ; Distributed lock backed by Redis
       "conductor.app.workflow-execution-lock-enabled" true
       "conductor.workflow-execution-lock.type" "orkes_lock"
       ; Default lease is 1m, but we want to stress this, so we'll go short
       "conductor.workflow.lockLeaseTime" "1s"

       ; Misc engine settings
       "conductor.secrets.type" "memory"
       "conductor.id.generator" "time_based"

       ; UI
       "conductor.enable.ui.serving" true
       "conductor.ui.playground" false

       ; Limits
       "conductor.limits.maxWorkflowSizeInMiB" 20
       "conductor.limits.maxTaskSizeInKiB" 20000

       ; Metrics / actuator (what's an actuator?)
       "management.endpoints.web.exposure.include" "prometheus,health,mappings"

       ; Not sure what this does; should we lower it?
       ;"conductor.redis.eventExecutionPersistenceTTL" 86400

       ; TODO: check on these too
       ;"conductor.postgres.workflowDefCacheRefreshInterval" 60
       ;"conductor.postgres.taskDefCacheRefreshInterval"     60
       ;"conductor.postgres.execution.caching.enabled"       true

       ;"conductor.workflow.indexing.indexOffTimeInSec"      2
       ;"conductor.archive.db.indexer.indexOffTimeInSec"     2
       }
      properties-file)))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test node)
    (configure! test node)
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf
                  log-file
                  (str log-file ".plain")
                  pid-file)))

  db/LogFiles
  (log-files [this test node]
    ; I am once again asking people to stop logging ANSI color escape sequences
    ; when not connected to a pty
    (c/su
      (let [plain-file (str log-file ".plain")]
        (meh (c/exec :ansifilter :--max-size "16G" :-p :-i log-file :-o plain-file))
        {plain-file       "server.log"
         properties-file  "server.properties"})))

  db/Kill
  (start! [this test node]
    (c/su (cu/start-daemon!
            {:env {"LOCAL_HOST_IP" (cn/local-ip)}
             :chdir dir
             :pidfile pid-file
             :logfile log-file}
            "/usr/bin/java"
            "-Xms2g" "-Xmx4g"
            (str "-DCONDUCTOR_CONFIG_FILE=" properties-file)
            "-jar"
            jar-file)))

  (kill! [this test node]
     (c/su (cu/stop-daemon! pid-file)))

  db/Pause
  (pause! [this test node]
    (c/su (cu/stop-daemon! nil pid-file :STOP)))

  (resume! [this test node]
    (c/su (cu/stop-daemon! nil pid-file :CONT))))

(defn db
  "Constructs a server DB."
  [opts]
  (map->DB {}))

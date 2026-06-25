(ns jepsen.conductor.db.server
  "Database automation for the Conductor server."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java [io :as io]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [fs-cache :as fs-cache]
                    [random :as rand]
                    [role :as role]]
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

(defn install!
  "Installs the server."
  [test node]
  (debian/install [:openjdk-25-jdk-headless])
  (c/su (c/exec :mkdir :-p dir))
        (info "Uploading server")
        (c/upload (str (:version test) "/lib/server.jar")
                  jar-file))

(defn configure!
  "Writes out the properties file."
  [test node]
  (c/su
    (write-properties!
      {; DB
       "spring.datasource.url" (str "jdbc:postgresql://"
                                    (-> test :roles :db rand/nth)
                                    ":5432/")
       ; These are the credentials jepsen.postgres provisions by default
       "spring.datasource.username" "postgres"
       "spring.datasource.password" "pw"
       "spring.datasource.hikari.maximum-pool-size" 64

       ; Security
       "conductor.security.default.encryptionKey" "abcdefg"
       "conductor.security.enabled" "false"
       "conductor.security.jwt.secret" "amVwc2Vu" ; Base64 "jepsen"
       "conductor.security.jwt.exp" 3600

       ; Automatically create admin users on authentication; we're just here to
       ; do safety testing
       "conductor.security.default.create-user-on-authentication" true
       "conductor.security.default.roles" "ADMIN"

       ; Worker keys
       "conductor.security.default.workers.accessKeyId" worker-key
       "conductor.security.default.workers.accessKeySecret" worker-secret

       "conductor.enable.ui.serving"        true
       "conductor.ui.event.monitor.enabled" true
       ; Not sure what this does; should we lower it?
       "conductor.redis.eventExecutionPersistenceTTL" 86400

       ; These are marked "optimizations" in the example config
       "conductor.workflow-execution-lock.type" "local_only"
       ; TODO: definitely want to mess with this
       "conductor.app.lockLeaseTime"                  120000
       "conductor.workflow.execution.maxTimeInMillis" 10000
       "conductor.app.lockTimeToTry"                  10
       "conductor.app.sweeper.sweepBatchSize"         10
       "conductor.app.sweeperThreadCount"             10
       ; TODO: investigate and tune this
       "conductor.app.workflowOffsetTimeout"     30

       "conductor.app.eventProcessorThreadCount" 200

       ; TODO: check on these too
       "conductor.postgres.workflowDefCacheRefreshInterval" 60
       "conductor.postgres.taskDefCacheRefreshInterval"     60
       "conductor.postgres.execution.caching.enabled"       true

       "conductor.workflow.indexing.indexOffTimeInSec"      2
       "conductor.archive.db.indexer.indexOffTimeInSec"     2

       ; Gosh these seem high
       "server.tomcat.threads.max"              200
       "server.tomcat.threads.min-spare"        50
       "conductor.app.event.processing.enabled" true

       ; Clean up job configurations
       "conductor.postgres.eventExecutionCleanupEnabled"     true
       "conductor.postgres.eventExecutionCleanupFrequency"   5
       "conductor.postgres.eventExecutionCleanupBatchSize"   1000
       "conductor.postgres.eventExecutionCleanupThreadCount" 2
       "conductor.script.evaluator.contextPoolEnabled"       true

       ; Partition manager
       ; TODO: change this?
       "conductor.archive.db.partitions.rowsThreshold" 50000000
       ; Always clean up
       "conductor.archive.db.partitions.minPartitionsForCleanup" 0
       "conductor.archive.db.partitions.retentionDays" 30

       ; Disable indexing worker
       ; TODO: change this?
       "conductor.archive.db.indexer.enabled" false

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
        (c/exec :ansifilter :--max-size "16G" :-p :-i log-file :-o plain-file)
        {plain-file       "server.log"
         properties-file  "server.properties"})))

  db/Kill
  (start! [this test node]
    (c/su (cu/start-daemon!
            {:env {"SPRING_PROFILES_ACTIVE" "dev"
                   "LOCAL_HOST_IP" (cn/local-ip)}
             :chdir dir
             :pidfile pid-file
             :logfile log-file}
            "/usr/bin/java"
            "-Xms2g" "-Xmx4g"
            (str "-DCONDUCTOR_CONFIG_FILE=" properties-file)
            ; Not sure why this is in both the env and here, but... that's what
            ; the example script does!
            "-DSPRING_PROFILES_ACTIVE=dev"
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
  []
  (DB.))

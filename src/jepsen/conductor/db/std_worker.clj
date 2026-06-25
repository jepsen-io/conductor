(ns jepsen.conductor.db.std-worker
  "Database automation for the standard worker process that ships with
  Conductor."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [fs-cache :as fs-cache]
                    [role :as role]]
            [jepsen.conductor.base :refer :all]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian])
  (:import (java.io File)
           (java.nio.file StandardOpenOption)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel
                              FileChannel$MapMode)
           (java.security MessageDigest)
           (java.util Base64)))

(def dir
  "Where do we install the worker remotely?"
  "/opt/conductor-std-worker")

(def jar-file
  "Where do we put the jar file?"
  (str dir "/std-worker.jar"))

(def properties-file
  "Where do we write worker.properties?"
  (str dir "/worker.properties"))

(def log-file
  "Where do we log daemon output?"
  (str dir "/std-worker.log"))

(def pid-file
  "Where do we store the pidfile?"
  (str dir "/std-worker.pid"))

(defn install!
  "Installs the worker."
  [test node]
  (debian/install [:openjdk-25-jdk-headless])
  (c/su (c/exec :mkdir :-p dir))
        (info "Uploading standard worker")
        (c/upload (str (:version test) "/lib/worker.jar")
                  jar-file))

(defn configure!
  "Writes out the properties file."
  [test node]
  (c/su
    (write-properties!
      {"conductor.server.url" (worker-server-url test node)
       "conductor.security.client.key-id" worker-key
       "conductor.security.client.secret" worker-secret
       "conductor.eventHandlerConsumer.nats.asyncProcessing" true
       ; These were commented "optimization for event tasks"
       "conductor.worker.EVENT.threadCount" 2
       ; Stock is one JDBC thread, but why buy one when you can have two for
       ; twice the price?
       "conductor.worker.JDBC.threadCount" 2}
      properties-file)))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test node)
    (configure! test node)
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf log-file pid-file)))

  db/LogFiles
  (log-files [this test node]
    {log-file "std-worker.log"})

  db/Kill
  (start! [this test node]
    (c/su (cu/start-daemon!
            {:env {"SPRING_PROFILES_ACTIVE" "dev"}
             :chdir dir
             :pidfile pid-file
             :logfile log-file}
            "/usr/bin/java"
            "-Xms2g" "-Xmx4g"
            (str "-DCONDUCTOR_CONFIG_FILE=" properties-file)
            "-DtaskUpdateV2=true"
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
  "Constructs a standard worker DB."
  []
  (DB.))

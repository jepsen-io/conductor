(ns jepsen.conductor.db.std-worker
  "Database automation for the standard worker process that ships with
  Conductor."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java [io :as io]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [fs-cache :as fs-cache]
                    [role :as role]
                    [util :as util]]
            [jepsen.conductor.base :refer :all]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [org.httpkit.client :as http]))

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
  (debian/install [:openjdk-25-jdk-headless :curl])
  (c/su (c/exec :mkdir :-p dir))
        (info "Uploading standard worker")
        (c/upload (str (:version test) "/lib/worker.jar")
                  jar-file))

(defn configure!
  "Writes out the properties file."
  [test node]
  (c/su
    (write-properties!
      {
       "conductor.server.url" (worker-server-url test node)
       ; We run without auth
       ;"conductor.security.client.key-id" worker-key
       ;"conductor.security.client.secret" worker-secret
       }
      properties-file)))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test node)
    (configure! test node)
    ; Conductor seems to throw itself into a tight loop trying to reconnect if
    ; the server isn't available on startup. To save ourselves from generating
    ; ~15,000 lines of stacktrace per second, we'll wait until the node is
    ; available
    (let [url (str/replace (worker-server-url test node)
                           #"/api$"
                           "/actuator/health")]
      (util/await-fn
        (fn alive? []
          (let [r @(http/get url {:timeout 1000
                                  :connect-timeout 1000})]
            (when-let [err (:error r)]
              (throw err))
            r))
        {:log-message (str "Waiting for server API (" url
                           ") before starting")
         :log-interval 10000}))
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf
                  log-file
                  (str log-file ".plain")
                  pid-file)))

  db/LogFiles
  (log-files [this test node]
    (c/su
      ; We have to strip out all the ANSI color escapes if we ever want a hope
      ; of reading these files
      (let [plain-file (str log-file ".plain")]
        (c/exec :ansifilter :--max-size "16G" :-p :-i log-file :-o plain-file)
        {plain-file "std-worker.log"
         properties-file "std-worker.properties"})))

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
  "Constructs a standard worker DB."
  []
  (DB.))

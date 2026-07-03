(ns jepsen.conductor.db.worker
  "Database automation for worker processes."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [fs-cache :as fs-cache]
                    [role :as role]]
            [jepsen.conductor [core :as core]
                              [base :refer :all]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian])
  (:import (java.io File)
           (java.nio.file StandardOpenOption)
           (java.nio ByteBuffer)
           (java.nio.channels FileChannel
                              FileChannel$MapMode)
           (java.security MessageDigest)
           (java.util Base64)))

(def project-dir
  "Where does the worker Leiningen project live, relative to the root of this
  repo?"
  "worker")

(def dir
  "Where do we install the worker remotely?"
  "/opt/conductor-worker")

(def jar-file
  "Where do we put the jar file?"
  (str dir "/worker.jar"))

(def log-file
  "Where do we log daemon output?"
  (str dir "/worker.log"))

(def pid-file
  "Where do we store the pidfile?"
  (str dir "/worker.pid"))

(defn project-hash
  "Computes a hash of a lein project directory. Only cares about .clj and .java
  files. We use this to avoid rebuilding jars when the project hasn't changed."
  [dir]
  (let [md (->> (io/file dir)
                file-seq
                (filter (fn [^File f] (re-find #"\.(clj|java)$" (.getName f))))
                sort
                (reduce (fn [^MessageDigest md, ^File f]
                          (with-open [fc (FileChannel/open
                                           (.toPath f)
                                           (into-array [StandardOpenOption/READ]))]
                            (let [size (.size fc)
                                  buf  (.map fc FileChannel$MapMode/READ_ONLY
                                             0 size)]
                              (.update md buf)
                              md)))
                        (MessageDigest/getInstance "MD5")))]
    (.encodeToString (Base64/getEncoder)
                     (.digest ^MessageDigest md))))

(defn ^File build!
  "Builds the local worker into a fat JAR. Returns a File for the jar."
  []
  (info "Building worker...")
  (let [r (sh "lein" "uberjar"
              :dir project-dir
              :env (-> (into {} (System/getenv))
                       (dissoc "CLASSPATH")
                       (assoc "LEIN_SNAPSHOTS_IN_RELEASE" "TRUE")))]
    (if-let [[m file] (re-find #"Created (.+?-standalone\.jar)" (:out r))]
      (io/file file)
      (throw+ (merge r {:type :worker-build-failed
                        :cmd "lein uberjar"})))))

(defn ^File worker-jar!
  "Returns a local File containing the far jar for the worker."
  []
  (let [version (project-hash project-dir)
        path [:conductor :worker version]]
    (fs-cache/locking path
      (when-not (fs-cache/cached? path)
        (let [jar (build!)]
          (fs-cache/save-file! jar path)))
      (fs-cache/load-file path))))

(defn install!
  "Installs the worker."
  [test node]
  (debian/install [:openjdk-25-jdk-headless])
  (c/su (c/exec :mkdir :-p dir))
        (info "Uploading worker")
        (c/upload (worker-jar!) jar-file)
        (info "Uploaded worker"))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test node)
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf
                  log-file
                  pid-file
                  (str dir "/incarnation"))))

  db/LogFiles
  (log-files [this test node]
    {log-file "worker.log"})

  db/Kill
  (start! [this test node]
    (c/su (cu/start-daemon!
            {:env {"CONDUCTOR_SERVER_URL" (worker-server-url test node)
                   "JEPSEN_NODE" node
                   "JEPSEN_HISTORY_URL"
                   (str "http://" (cn/control-ip) ":" core/history-port "/")}
             :chdir dir
             :pidfile pid-file
             :logfile log-file}
            "/usr/bin/java"
            "-jar"
            jar-file)))

  (kill! [this test node]
     (c/su (cu/stop-daemon! "java" pid-file)))

  db/Pause
  (pause! [this test node]
    (c/su (cu/stop-daemon! "java" pid-file :STOP)))

  (resume! [this test node]
    (c/su (cu/stop-daemon! "java" pid-file :CONT))))

(defn db
  "Constructs a worker DB."
  []
  (DB.))

(ns jepsen.conductor.base
  "Basic support functions."
  (:require [clojure [string :as str]]
            [jepsen.control.util :as cu]))

(def worker-key
  "jepsen-worker")

(def worker-secret
  "jepsen-worker-secret")

(defn write-properties!
  "Writes out a Java .properties file given a map."
  [m file]
  (cu/write-file!
    (->> m
         (map (fn [[k v]] (str k "=" v)))
         (str/join "\n"))
    file))

(defn server-url
  "Takes a test and an integer i. Returns the server url of the i'th conductor
  server, modulo the number of conductor servers. We use this to stripe clients
  and workers across various servers."
  [test i]
  (let [nodes (-> test :roles :conductor)
        node (nth nodes (mod i (count nodes)))]
    (str "http://" node ":8080/api")))

(defn worker-server-url
  "Takes a test and a worker node. Returns the corresponding server that worker
  should talk to."
  [test worker-node]
  (server-url test (.indexOf ^java.util.List (:nodes test) worker-node)))

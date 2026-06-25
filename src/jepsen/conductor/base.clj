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
         sort
         (str/join "\n"))
    file))

(def server-port
  "The port for server URLs."
  8080)

(defn server-url
  "Takes a test and an integer i. Returns the server url of the i'th conductor
  server, modulo the number of conductor servers. We use this to stripe clients
  and workers across various servers."
  [test i]
  (let [nodes (-> test :roles :server)
        node (nth nodes (mod i (count nodes)))]
    (str "http://" node ":" server-port "/api")))

(defn worker-server-url
  "Takes a test and a worker node. Returns the corresponding server that worker
  should talk to."
  [test worker-node]
  (let [i (.indexOf ^java.util.List (:nodes test) worker-node)]
    (assert (not (neg? i)))
    (server-url test i)))

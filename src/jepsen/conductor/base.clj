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

(defn nth-node
  "Takes a test, a role, and an integer n. Returns a node with the given role,
  depending on n mod node-count. We use this to stripe servers/clients/etc
  across one another."
  [test role i]
  (let [nodes (-> test :roles (get role))]
    (nth nodes (mod i (count nodes)))))

(defn server-url
  "Takes a test and an integer i. Returns the server url of the i'th conductor
  server, modulo the number of conductor servers. We use this to stripe clients
  and workers across various servers."
  [test i]
  (let [nodes (-> test :roles :server)
        node (nth nodes (mod i (count nodes)))]
    (str "http://" node ":" server-port "/api")))

(defn node-index
  "Returns the index of the given node in the given test."
  [test node]
  (let [i (.indexOf ^java.util.List (:nodes test) node)]
    (assert (not (neg? i)))
    i))

(defn worker-server-url
  "Takes a test and a worker node. Returns the corresponding server that worker
  should talk to."
  [test worker-node]
  (server-url test (node-index test worker-node)))

(defn terminal-status?
  "Is this status keyword terminal, or could it change later?"
  [status]
  (case status
    (:running :paused) false
    (:completed :failed :timed-out :terminated) true))

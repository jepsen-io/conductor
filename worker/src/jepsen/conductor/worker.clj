(ns jepsen.conductor.worker
  (:import (io.orkes.conductor.client ApiClient)
           (com.netflix.conductor.client.http TaskClient)))

(defn api-client
  "Opens a new Conductor API client."
  []
  (ApiClient. (System/getenv "CONDUCTOR_SERVER_URL")
              (System/getenv "CONDUCTOR_AUTH_KEY")
              (System/getenv "CONDUCTOR_AUTH_SECRET")))

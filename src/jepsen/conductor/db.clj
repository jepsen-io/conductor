(ns jepsen.conductor.db
  "Database automation."
  (:require [clojure.tools.logging :refer [info warn]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [role :as role]]
            [jepsen.control.util :as cu]
            [jepsen.conductor.db [server :as server]
                                 [std-worker :as std-worker]
                                 [worker :as worker]]
            [jepsen.os.debian :as debian]
            [jepsen.postgres.db :as postgres]
            [jepsen.redis.cli :as redis]))

(defn db
  "Constructs a multi-role DB for our combined Postgres, Redis, Conductor, and
  Workers."
  [opts]
  (role/db
    {:postgres   (postgres/db {})
     :redis      (db/map-test (fn [test]
                                (assoc test
                                       :username "conductor"
                                       :password "pw"))
                                ((get redis/db-types (:redis-type opts)) {}))
     :server     {:db (server/db {})
                  :deps [:postgres :redis]}
     ; These do depend on the server, but they'll do their own coordination.
     :std-worker (std-worker/db)
     :worker     (worker/db)}))

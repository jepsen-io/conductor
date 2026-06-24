(ns jepsen.conductor.db
  "Database automation."
  (:require [clojure.tools.logging :refer [info warn]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [role :as role]]
            [jepsen.control.util :as cu]
            [jepsen.conductor.db [conductor :as conductor]
                                 [worker :as worker]]
            [jepsen.os.debian :as debian]
            [jepsen.postgres.db :as postgres]
            [jepsen.redis.db.single :as redis]))

(defn db
  "Constructs a multi-role DB for our combined Postgres, Redis, Conductor, and
  Workers."
  [opts]
  (role/db
    {:db        (postgres/db {})
     :queue     (redis/db)
     :conductor (conductor/db)
     :worker    (worker/db)}))

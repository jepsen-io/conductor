(ns jepsen.conductor.db.conductor
  "Database automation for Conductor servers."
  (:require [clojure.tools.logging :refer [info warn]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
                    [db :as db]
                    [role :as role]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defrecord DB []
  db/DB
  (setup! [this test node]
    )

  (teardown! [this test node]
    )

  db/LogFiles
  (log-files [this test node]
    {})

  db/Kill
  (start! [this test node]
    )

  (kill! [this test node]
    )

  db/Pause
  (pause! [this test node]
          )

  (resume! [this test node]
           ))

(defn db
  "Constructs a Conductor DB."
  []
  (DB.))

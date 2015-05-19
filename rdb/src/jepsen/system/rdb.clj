(ns jepsen.system.rdb
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.codec             :as codec]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [jepsen.generator         :as gen]
            [jepsen.os.debian         :as debian]
            [knossos.core             :as knossos]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]))

(defn copy-from-home [file]
  (c/scp* (str (System/getProperty "user.home") "/" file) "/home/ubuntu"))

(defn db []
  (reify db/DB
    (setup! [this test node]
      (debian/install [:libprotobuf7 :libicu48 :psmisc])
      (let [file "rethinkdb/build/debug/rethinkdb"]
        (info node (str "Copying ~/" file "..."))
        (copy-from-home file)
        (info node (str "Copying ~/" file " DONE!")))
      (info node "Starting...")
      ;; TODO: detect server failing to start.
      (c/ssh* {:cmd "
rm -rf rethinkdb_data
chmod a+x rethinkdb
nohup ./rethinkdb -n `hostname` \\
                  --bind all \\
                  -j n1:29015 -j n2:29015 -j n3:29015 -j n4:29015 -j n5:29015 \\
                  >1.out 2>2.out &
sleep 1
tail -n+0 -F 1.out \\
  | grep --line-buffered '^Server ready' \\
  | while read line; do pkill -P $$ tail; exit 0; done
sleep 1
"})
      (info node "Starting DONE!")
      (if (= node :n1)
        (do
          (info node "Creating table...")
          (info node (str `(connect :host ~(name node) :port 28015)))
          (with-open [conn (connect :host (name node) :port 28015)]
            (r/run (r/db-create "jepsen") conn)
            (r/run (r/table-create (r/db "jepsen") "cas" {:replicas 5}) conn)
            (r/run (r/insert (r/table (r/db "jepsen") "cas") {:id 0 :val nil}) conn)
            (pr (r/run
                  (r/update
                   (r/table (r/db "rethinkdb") "table_config")
                   ;; Point 1
                   ;; {:write_acks "single"})
                   {:shards [{:primary_replica "n5"
                              :replicas ["n1" "n2" "n3" "n4" "n5"]}]})
                  conn))
            (r/run
              (rethinkdb.query-builder/term :WAIT [(r/table (r/db "jepsen") "cas")] {})
              conn))
          (info node "Creating table DONE!"))
        (info node "Not creating table.")))
    (teardown! [_ test node]
      (info node "Tearing down db...")
      (c/ssh* {:cmd "
killall rethinkdb
f=0
while [[ \"`ps auxww | grep rethinkdb | grep -v grep`\" ]] && [[ \"$f\" -lt 10 ]]; do
  sleep 1
  f=$((f+1))
done
if [[ \"$f\" -ge 10 ]]; then
  killall -9 rethinkdb
fi
"})
      (info node "Tearing down db DONE!"))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (info node "Connecting CAS Client...")
    (info node (str `(connect :host ~(name node) :port 28015)))
    (let [conn (connect :host (name node) :port 28015)]
      (info node "Connecting CAS Client DONE!")
      (assoc this :conn conn :node node)))

  (invoke! [this test op]
    (let [fail (if (= :read (:f op)) :fail :info)
          row (r/get
               (rethinkdb.query-builder/term
                :TABLE
                [(r/db "jepsen") "cas"]
                ;; Point 2
                {:use_outdated false})
               0)]
      (try+
       (case (:f op)
         :read (assoc
                op
                :type :ok
                :value (r/run (r/get-field row "val") (:conn this)))
         :write (do (r/run
                      (r/update row {:val (:value op)})
                      (:conn this))
                    (assoc op :type :ok))
         :cas (let [[value value'] (:value op)
                    res (r/run
                          (r/update
                           row
                           (r/fn [row]
                             (r/branch
                              (r/eq (r/get-field row "val") value)
                              {:val value'}
                              (r/error "abort"))))
                          (:conn this))]
                (assoc op :type (if (= (:errors res) 0) :ok :fail))))
       (catch java.net.SocketTimeoutException e
         (assoc op :type fail :value :timed-out))
       (catch (and (instance? clojure.lang.ExceptionInfo %)) e
         (assoc op :type fail :value e))
       (catch (and (:errorCode %) (:message %)) e
         (assoc op :type fail :value e)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single etcd node."
  []
  (CASClient. "jepsen" nil))

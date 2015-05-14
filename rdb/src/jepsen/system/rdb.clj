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

;; (def binary "/opt/etcd/bin/etcd")
;; (def pidfile "/var/run/etcd.pid")
;; (def data-dir "/var/lib/etcd")
;; (def log-file "/var/log/etcd.log")

;; (defn peer-addr [node]
;;   (str (name node) ":2380"))

;; (defn addr [node]
;;   (str (name node) ":2380"))

;; (defn cluster-url [node]
;;   (str "http://" (name node) ":2380"))

;; (defn listen-client-url [node]
;;   (str "http://" (name node) ":2379"))

;; (defn cluster-info [node]
;;   (str (name node) "=http://" (name node) ":2380"))

;; (defn peers
;;   "The command-line peer list for an etcd cluster."
;;   [test]
;;   (->> test
;;        :nodes
;;        (map cluster-info)
;;        (str/join ",")))

;; (defn running?
;;   "Is etcd running?"
;;   []
;;   (try
;;     (c/exec :start-stop-daemon :--status
;;             :--pidfile pidfile
;;             :--exec binary)
;;     true
;;     (catch RuntimeException _ false)))

;; (defn start-etcd!
;;   [test node]
;;   (info node "starting etcd")
;;   (c/exec :start-stop-daemon :--start
;;           :--background
;;           :--make-pidfile
;;           :--pidfile        pidfile
;;           :--chdir          "/opt/etcd"
;;           :--exec           binary
;;           :--no-close
;;           :--
;;           :-data-dir        data-dir
;;           :-name            (name node)
;;           :-advertise-client-urls (cluster-url node)
;;           :-listen-peer-urls (cluster-url node)
;;           :-listen-client-urls (listen-client-url node)
;;           :-initial-advertise-peer-urls (cluster-url node)
;;           :-initial-cluster-state "new"
;;           :-initial-cluster (peers test)
;;           :>>               log-file
;;           (c/lit "2>&1")))

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
      (c/ssh* {:cmd "
rm -rf rethinkdb_data
chmod a+x rethinkdb
nohup ./rethinkdb --bind all \\
                  -j n1:29015 -j n2:29015 -j n3:29015 -j n4:29015 -j n5:29015 \\
                  >1.out 2>2.out &
sleep 5
"})
      (info node "Starting DONE!")
      (if (= node :n1)
        (do
          (info node "Creating table...")
          (info node (str `(connect :host ~(name node) :port 28015)))
          (with-open [conn (connect :host (name node) :port 28015)]
            (r/run (r/db-create "jepsen") conn)
            (r/run (r/table-create (r/db "jepsen") "cas") conn)
            (r/run (r/insert (r/table (r/db "jepsen") "cas") {:id 0 :val nil}) conn))
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
  ;;    [client (v/connect (str "http://" (name node) ":2379"))]
  ;;      (v/reset! client k (json/generate-string nil))
  ;;      (assoc this :client client)))

  (invoke! [this test op]
    (let [fail (if (= :read (:f op)) :fail :info)
          row (r/get (r/table (r/db "jepsen") "cas") 0)]
      (try+
       (case (:f op)
         :read (assoc op :type :ok :value (r/run (r/get-field row "val") (:conn this)))
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

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
      (let [file "rethinkdb/build/debug/rethinkdb"]
        (info node (str "Copying ~/" file "..."))
        (copy-from-home file)
        (info node (str "Copying ~/" file " DONE!")))
      (info node (c/ssh* {:cmd "hostname"})))
    (teardown! [_ test node]
      (info node "Tearing down db...")
      (info node "Tearing down db DONE!"))))

(def glob (atom nil))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (info node "Connecting CAS Client...")
    (info node "Connecting CAS Client DONE!")
    (assoc this :client client))
  ;;    [client (v/connect (str "http://" (name node) ":2379"))]
  ;;      (v/reset! client k (json/generate-string nil))
  ;;      (assoc this :client client)))
  (invoke! [this test op]
    (let [fail (if (= :read (:f op)) :fail :info)]
      (try+
       (case (:f op)
         :read (assoc op :type :ok :value @glob)
         :write (do (swap! glob (fn [_] (:value op)))
                    (assoc op :type :ok))
         :cas   (let [[value value'] (:value op)
                      ok? (if (= @glob value)
                            (swap! glob (fn [_] value')))]
                  (assoc op :type (if ok? :ok :fail))))
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

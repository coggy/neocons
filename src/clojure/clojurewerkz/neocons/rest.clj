(ns clojurewerkz.neocons.rest
  (:import  [java.net URI])
  (:require [clj-http.client   :as http]
            [clojure.data.json :as json])
  (:use     [clojurewerkz.support.http.statuses]
            [clojurewerkz.neocons.rest.helpers :only [maybe-append]]))

;;
;; Implementation
;;

(def ^{ :dynamic true } *endpoint*)

(defn- var-or-nil [v]
  (try v (catch IllegalStateException e nil)))

(defn- with-creds [options]
  (let [point (var-or-nil *endpoint*)]
	(if (nil? point) options
	  (assoc options :basic-auth [(:login point) (:pwd point)]))))

(defn GET
  [^String uri & { :as options }]
  (io!
   (http/get uri (with-creds (merge options { :accept :json })))))

(defn POST
  [^String uri &{ :keys [body] :as options }]
  (io!
   (http/post uri (with-creds (merge options { :accept :json :content-type :json :body body })))))

(defn PUT
  [^String uri &{ :keys [body] :as options }]
  (io!
   (http/put uri (with-creds (merge options { :accept :json :content-type :json :body body })))))

(defn DELETE
  [^String uri &{ :keys [body] :as options }]
  (io!
   (http/delete uri (with-creds (merge options { :accept :json })))))

(defrecord Neo4JEndpoint
    [version node-uri relationships-uri node-index-uri relationship-index-uri relationship-types-uri batch-uri extensions-info-uri extensions reference-node-uri uri login pwd])


(defn- do-init-get [uri login password] 
  (io!
   (http/get uri { :accept :json :basic-auth [login password] })))

;;
;; API
;;

(defprotocol Connection
  (connect  [uri login pwd] "Connects to given Neo4J REST API endpoint and performs service discovery")
  (connect! [uri login pwd] "Connects to given Neo4J REST API endpoint, performs service discovery and mutates *endpoint* state to store it"))

(extend-protocol Connection
  String
  (connect [uri ^String login ^String pwd]
    (let [{ :keys [status body] } (do-init-get uri login pwd)]
      (if (success? status)
        (let [payload (json/read-json body true)]
          (Neo4JEndpoint. (:neo4j_version      payload)
                          (:node               payload)
                          (str uri (if (.endsWith uri "/")
                                     "relationship"
                                     "/relationship"))
                          (:node_index         payload)
                          (:relationship_index payload)
                          (:relationship_types payload)
                          (:batch              payload)
                          (:extensions_info    payload)
                          (:extensions         payload)
                          (:reference_node     payload)
                          (maybe-append uri "/")
						  login 
						  pwd)))))
  (connect! [uri ^String login ^String pwd]
    (defonce ^{ :dynamic true } *endpoint* (connect uri login pwd))))

(extend-protocol Connection
  URI
  (connect [uri login pwd]
    (connect (.toString uri) (str login) (str pwd)))
  (connect! [uri login pwd]
    (connect! (.toString uri) (str login) (str pwd))))
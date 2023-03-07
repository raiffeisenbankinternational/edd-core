(ns edd.request-cache
  (:require [lambda.request :as request]
            [clojure.walk :as clojure-walk]))

(defn- get-realm
  [ctx]
  (get-in ctx [:meta :realm]))

(defn get-aggregate
  [ctx id]
  (get-in @request/*request* [:edd-core
                              :cache
                              (get-realm ctx)
                              :aggregate
                              id]))

(defn clear
  []
  (swap! request/*request* #(assoc-in % [:edd-core
                                         :cache]
                                      nil)))

(defn update-aggregate
  "Here is very important to keywordize key. See more details on
  lambda.util/fix-keys"
  [ctx {:keys [id] :as aggregate}]
  (swap! request/*request*
         #(assoc-in % [:edd-core
                       :cache
                       (get-realm ctx)
                       :aggregate
                       id]
                    (clojure-walk/keywordize-keys aggregate))))

(defn get-identitiy
  [ctx id]
  (get-in @request/*request* [:edd-core
                              :cache
                              (get-realm ctx)
                              :identities
                              id]))

(defn store-identities
  [ctx identities]
  (let  [identities (if (vector? identities)
                      identities
                      [identities])]
    (swap! request/*request*
           #(update-in % [:edd-core
                          :cache
                          (get-realm ctx)
                          :identities]
                       (fn [i]
                         (reduce
                          (fn [p {:keys [identity id]}]
                            (assoc p identity id))
                          (or i {})
                          identities))))))

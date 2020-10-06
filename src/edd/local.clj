(ns edd.local
  (:require [edd.core :as edd]))



(defn local-cmd
  [reg-fn cmd]
  (edd/with-db-con
    (fn [ctx]
      (edd/dispatch-request
        (reg-fn ctx)
        {:commands [cmd]}))))

(defn local-query
  [reg-fn query]
  (edd/with-db-con
    (fn [ctx]
      (edd/dispatch-request
        (reg-fn ctx)
        {:query query}))))

(defn local-apply
  [reg-fn apply]
  (edd/with-db-con
    (fn [ctx]
      (edd/dispatch-request
        (reg-fn ctx)
        {:apply apply}))))

(defn local-fn
  [ctx fn & params]
  (apply edd/with-db-con
         fn params))

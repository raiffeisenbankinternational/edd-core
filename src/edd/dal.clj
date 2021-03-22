(ns edd.dal
  (:require [clojure.tools.logging :as log]))

(defmulti get-events
  (fn [{:keys [id] :as ctx}]
    (:event-store ctx)))

(defmulti get-sequence-number-for-id
  (fn [{:keys [id] :as ctx}]
    (:event-store ctx)))

(defmulti get-id-for-sequence-number
  (fn [{:keys [sequence] :as ctx}]
    (:event-store ctx)))

(defmulti get-aggregate-id-by-identity
  (fn [{:keys [identity] :as ctx}]
    (:event-store ctx)))

(defmulti store-effects
  (fn [ctx] (:event-store ctx)))

(defmulti log-dps
  (fn [ctx] (:event-store ctx)))

(defmulti log-request
  (fn [{:keys [commands] :as ctx}]
    (:event-store ctx)))

(defmulti log-response
  (fn [ctx]
    (:event-store ctx)))

(defmulti get-max-event-seq
  (fn [{:keys [id] :as ctx}]
    (:event-store ctx)))

(defmulti get-command-response
  (fn [{:keys [request-id breadcrumbs] :as ctx}]
    (:event-store ctx)))

(defmulti store-results
  (fn [ctx] (:event-store ctx)))

(defmulti with-init
  (fn [ctx body-fn] (:event-store ctx)))

(defmethod with-init
  :default
  [ctx body-fn]
  (log/info "Default event init")
  (body-fn ctx))

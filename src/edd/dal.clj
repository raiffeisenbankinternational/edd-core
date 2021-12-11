(ns edd.dal
  (:require [clojure.tools.logging :as log]))

(defmulti get-events
  (fn [{:keys [_id] :as ctx}]
    (:event-store ctx :default)))

(defmethod get-events
  :default
  [_] [])

(defmulti get-sequence-number-for-id
  (fn [{:keys [_id] :as ctx}]
    (:event-store ctx)))

(defmulti get-id-for-sequence-number
  (fn [{:keys [_sequence] :as ctx}]
    (:event-store ctx)))

(defmulti get-aggregate-id-by-identity
  (fn [{:keys [_identity] :as ctx}]
    (:event-store ctx)))

(defmulti store-effects
  (fn [ctx] (:event-store ctx)))

(defmulti log-dps
  (fn [ctx] (:event-store ctx)))

(defmulti log-request
  (fn [{:keys [_commands] :as ctx} _body]
    (:event-store ctx)))

(defmulti log-request-error
  (fn [{:keys [_commands] :as ctx} _body _error]
    (:event-store ctx)))

(defmulti log-response
  (fn [ctx]
    (:event-store ctx)))

(defmulti get-max-event-seq
  (fn [{:keys [_id] :as ctx}]
    (:event-store ctx)))

(defmulti get-command-response
  (fn [{:keys [_request-id _breadcrumbs] :as ctx}]
    (:event-store ctx)))

(defmulti store-results
  (fn [ctx] (:event-store ctx)))

(defmulti with-init
  (fn [ctx _body-fn] (:event-store ctx)))

(defmethod with-init
  :default
  [ctx body-fn]
  (log/info "Default event init")
  (body-fn ctx))

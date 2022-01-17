(ns edd.response.cache
  (:require [clojure.tools.logging :as log]
            [lambda.test.fixture.state :as state]
            [clojure.string :as str]))

(defmulti cache-response
  (fn [ctx _]
    (:response-cache ctx)))

(defmethod cache-response
  :default
  [{:keys [service-name breadcrumbs request-id]
    :or   {service-name ""}}
   {:keys [idx] :as resp}]
  (log/info "No response cache implementation")
  (let [key (str "response/"
                 request-id
                 "/"
                 (str/join "-" breadcrumbs)
                 "/"
                 (name service-name)
                 (when idx
                   (str "-part." idx))
                 ".json")]
    (swap! state/*dal-state* assoc key resp)
    {:key key}))

(defn register-default
  [ctx]
  (assoc ctx :response-cache :none))
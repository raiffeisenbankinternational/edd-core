(ns edd.el.query
  (:require [lambda.util :as util]
            [lambda.request :as req]
            [clojure.tools.logging :as log]
            [aws.aws :as aws]
            [malli.core :as m]
            [edd.ctx :as edd-ctx]
            [edd.schema :as schema]
            [lambda.http-client :as http-client]))

(defn calc-service-query-url
  [service]
  (str "https://api."
       (util/get-env "PrivateHostedZoneName")
       "/legacy/"
       (name service)
       "/query"))

(defn call-query-fn
  [_ cmd query-fn deps]
  (if (fn? query-fn)
    (query-fn deps cmd)
    query-fn))

(defn -http-call
  [ctx {:keys [service query]}]
  (let [token (aws/get-token ctx)
        service-name (:service-name ctx)
        url (calc-service-query-url
             service)]
    (when (:query-id query)
      (http-client/retry-n
       #(util/http-post
         url
         (http-client/request->with-timeouts
          %
          {:body    (util/to-json
                     {:query          query
                      :meta           (:meta ctx)
                      :request-id     (:request-id ctx)
                      :interaction-id (:interaction-id ctx)})
           :headers {"Content-Type"    "application/json"
                     "X-Authorization" token}}
          :idle-timeout 10000))
       :meta {:to-service   service
              :from-service service-name
              :query-id     (:query-id query)}))))

(defn with-cache
  [request f]
  (if (req/is-scoped)
    (if-let [hint
             (get-in
              @req/*request*
              [:edd-core :http-cache request])]
      hint
      (do
        (swap! req/*request*
               #(assoc-in % [:edd-core
                             :http-cache
                             request] (f request)))
        (get-in
         @req/*request*
         [:edd-core :http-cache request])))
    (f request)))

(defn resolve-remote-dependency
  [ctx cmd {:keys [service query]} deps]
  (log/info "Resolving remote dependency: "
            service
            (or (:cmd-id cmd)
                (:cmd-id cmd)))
  (let [query-fn query
        service-name (:service-name ctx)
        resolved-query (call-query-fn ctx cmd query-fn deps)
        request
        {:service service
         :query resolved-query}
        response (with-cache request (partial -http-call ctx))]

    (when (nil? (:query-id resolved-query))
      (log/info "Skiping resolving remote dependency because query-id is nil")
      nil)
    (when (:error response)
      (throw (ex-info (str "Error fetching dependency" service)
                      {:error {:to-service   service
                               :from-service service-name
                               :query-id     (:query-id resolved-query)
                               :message      (:error response)}})))
    (when (:error (get response :body))
      (throw (ex-info (str "Error response from service " service)
                      {:error {:to-service   service
                               :from-service service-name
                               :query-id     (:query-id resolved-query)
                               :message      {:response     (get response :body)
                                              :error-source service}}})))
    (if (> (:status response 0) 299)
      (throw (ex-info (str "Deps request error for " service)
                      {:error {:to-service   service
                               :from-service service-name
                               :service      service
                               :query-id     (:query-id resolved-query)
                               :status       (:status response)
                               :message      (str "Response status:" (:status response))}}))
      (get-in response [:body :result]))))

(defn- validate-response-schema-setting [ctx]
  (let [setting (or (:response-schema-validation ctx)
                    (:response-schema-validation (:meta ctx)))]
    (assert #{nil :log-on-error :throw-on-error} setting)
    setting))

(defn- validate-response-schema? [ctx]
  (contains? #{:log-on-error :throw-on-error}
             (validate-response-schema-setting ctx)))

(defn- maybe-validate-response [ctx produces response]
  (when (and produces
             (validate-response-schema? ctx))
    (let [wrapped {:result response}]
      (when-not (m/validate produces wrapped)
        (let [error (schema/explain-error produces wrapped)]
          (condp = (validate-response-schema-setting ctx)
            :throw-on-error
            (throw (ex-info "Invalid response" {:error    error
                                                :response wrapped}))
            :log-on-error
            (log/warnf "Invalid response %s" (pr-str {:error error
                                                      :response wrapped}))))))))

(declare handle-query)

(defn resolve-local-dependency
  [ctx cmd query-fn deps]
  (log/debug "Resolving local dependency")
  (let [query (call-query-fn ctx cmd query-fn deps)]
    (when query
      (let [resp (handle-query ctx {:query query})]
        (if (:error resp)
          (throw (ex-info "Failed to resolve local deps" {:error resp}))
          resp)))))

(defn fetch-dependencies-for-deps
  [ctx deps request]
  (let [deps (if (vector? deps)
               (partition 2 deps)
               deps)
        dps-value (reduce
                   (fn [p [key req]]
                     (log/info "Query for dependency" key (:service req "locally"))
                     (let [dep-value
                           (try (if (:service req)
                                  (resolve-remote-dependency
                                   ctx
                                   request
                                   req
                                   p)
                                  (resolve-local-dependency
                                   ctx
                                   request
                                   req
                                   p))
                                (catch AssertionError e
                                  (log/warn "Assertion error for deps " key)
                                  nil))]
                       (if dep-value
                         (assoc p key dep-value)
                         p)))
                   {}
                   deps)]
    dps-value))

(defn handle-query
  [ctx body]
  (let [query (:query body)
        query-id (keyword (:query-id query))
        {:keys [consumes
                handler
                produces]} (get-in ctx [:edd-core :queries query-id])]

    (log/debug "Handling query" query-id)
    (when-not handler
      (throw (ex-info "No handler found"
                      {:error    "No handler found"
                       :query-id query-id})))
    (when-not (m/validate consumes query)
      (throw (ex-info "Invalid request"
                      {:error (schema/explain-error consumes query)})))
    (let [{:keys [deps]} (edd-ctx/get-query ctx query-id)
          deps-value (fetch-dependencies-for-deps ctx deps query)
          ctx (merge ctx deps-value)
          resp (util/d-time
                (str "handling-query: " query-id)
                (handler ctx query))]
      (log/debug "Query response" resp)
      (maybe-validate-response ctx produces resp)
      resp)))

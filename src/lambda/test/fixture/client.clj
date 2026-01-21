(ns lambda.test.fixture.client
  (:require [clojure.test :refer [is]]
            [lambda.util :as util]
            [clojure.tools.logging :as log]
            [clojure.data :refer [diff]]
            [clojure.string :as str]
            [lambda.logging.state :as log-state]))

(def ^:dynamic *world*)

(defn map-body-to-edn
  [traffic]
  (if (:body traffic)
    (try (update traffic :body util/to-edn)
         (catch Exception _e
           traffic))
    traffic))

(defn map-body-to-edn-deep
  [traffic]
  (if (:body traffic)
    (try (-> (update traffic :body util/to-edn)
             (update-in [:body] map-body-to-edn-deep))
         (catch Exception _e
           traffic))
    traffic))

(defn verify-traffic-edn
  [expected]
  (is (= expected
         (->> (:traffic @*world*)
              (mapv map-body-to-edn)
              (mapv #(dissoc % :keepalive))))))

(defn verify-traffic
  [expected]
  (is (= expected
         (mapv
          #(dissoc % :keepalive)
          (:traffic @*world*)))))

(defn traffic-edn
  ([]
   (mapv map-body-to-edn-deep (:traffic @*world*)))
  ([n]
   (nth (traffic-edn) n)))

(defn traffic
  ([]
   (mapv map-body-to-edn (:traffic @*world*)))
  ([n]
   (nth (traffic) n)))

(defn responses []
  (map :body (traffic)))

(defn record-traffic
  [req]
  (let [clean-req (if (= (:method req)
                         :get)
                    (dissoc req :req)
                    req)]
    (swap! *world*
           #(update % :traffic
                    (fn [v]
                      (conj v clean-req))))))

(defn remove-at
  [coll idx]
  (vec (concat (subvec coll 0 idx)
               (subvec coll (inc idx)))))

(defn find-first
  [coll func]
  (first
   (keep-indexed (fn [idx v]
                   (when (func v) idx))
                 coll)))

(defn is-match
  [{:keys [url method body]} v]
  (and
   (= (get v method) url)
   (or (= (:req v) nil)
       (= (get v :req) body)
        ; We want :req to be subset of expected body
       (= (first
           (diff (get v :req)
                 (util/to-edn body)))
          nil))))

(defn- log-mock-not-found
  "Log mock not found error as a single message with indentation"
  [method url req all pass-through?]
  (let [depth (if (bound? #'log-state/*d-time-depth*) log-state/*d-time-depth* 0)
        indent (str "      "
                    (str/join "" (repeat depth "  ")))

        body-str (if (:body req)
                   (try
                     (str (util/to-edn (:body req)))
                     (catch Exception _
                       (str (:body req))))
                   "none")
        expected-urls (when (seq all)
                        (str/join "\n"
                                  (map-indexed
                                   (fn [i m]
                                     (str indent "    [" i "] " (or (:get m) (:post m) (:put m) (:delete m) (:patch m))))
                                   all)))
        prefix (if pass-through? "Mock pass-through" "Mock not found")
        message (str prefix "\n"
                     indent "Request:\n"
                     indent "  Method: " method "\n"
                     indent "  URL: " url "\n"
                     indent "  Body: " body-str "\n"
                     indent "Available mocks: " (count all)
                     (when expected-urls
                       (str "\n" indent "  Expected URLs:\n" expected-urls)))]
    (if pass-through?
      (log/warn message)
      (log/error message))))

(defn handle-request
  "Each request contained :method :url pair and response :body.
  Optionally there might be :req which is body of request that
  has to me matched"
  [{:keys [url method] :as req} & _rest]
  (record-traffic req)
  (let [all (:responses @*world*)
        idx (find-first
             all
             (partial is-match req))
        resp (get all idx)]
    (if idx
      (let [{:keys [reuse-responses]
             :or {reuse-responses false}} (:config @*world*)]
        (when-not reuse-responses
          (swap! *world*
                 update-in [:responses]
                 #(remove-at % idx)))
        (dissoc resp method :req :keep))
      (let [{:keys [pass-through original-request-fn]
             :or {pass-through false}} (:config @*world*)]
        (log-mock-not-found method url req all pass-through)
        (if (and pass-through original-request-fn)
          (original-request-fn req)
          {:status 200
           :body   (util/to-json {:result nil})})))))

(defmacro mock-http
  [responses & body]
  `(let [responses# ~responses
         config# (if (map? responses#)
                   (:config responses#)
                   {})
         config# (if (:pass-through config#)
                   (assoc config# :original-request-fn util/request)
                   config#)
         responses# (if (map? responses#)
                      (:responses responses#)
                      responses#)]
     (binding [*world* (atom {:config config#
                              :responses responses#})]
       (with-redefs [util/request handle-request]
         (do ~@body)))))

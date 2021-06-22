(ns sdk.aws.s3
  (:require [sdk.aws.common :as common]
            [lambda.util :as util]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn convert-query-params
  [params]
  (reduce
   (fn [p [k v]]
     (assoc p k v))
   {}
   params))

(defn- parse-response
  [response object]
  (log/debug "Auth response" response)
  (cond
    (contains? response :error) (do
                                  (log/error "Failed update" response)
                                  {:error (:error response)})
    (> (:status response 199) 299) (do
                                     (log/error "S3 Response failed"
                                                (:status response)
                                                (:body response))
                                     {:error {:status  (:status response)
                                              :message (slurp (:body response))
                                              :key     (get-in object [:s3 :object :key])
                                              :bucket  (get-in object [:s3 :bucket :name])}})
    :else response))

(defn put-object
  "puts object.content (should be plain string) into object.s3.bucket.name under object.s3.bucket.key"
  [object]
  (let [req {:method     "PUT"
             :uri        (str "/"
                              (get-in object [:s3 :bucket :name])
                              "/"
                              (get-in object [:s3 :object :key]))
             :headers    {"Host"                 "s3.eu-central-1.amazonaws.com"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (System/getenv "AWS_SESSION_TOKEN")}
             :service    "s3"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        common (common/authorize req)
        response (common/retry #(util/http-put
                                 (str "https://"
                                      (get (:headers req) "Host")
                                      (:uri req))
                                 {:as      :stream
                                  :headers (-> (:headers req)
                                               (dissoc "Host")
                                               (assoc "Authorization" common))
                                  :body    (get-in object [:s3 :object :content])
                                  :timeout 8000}
                                 :raw true)
                               3)
        {:keys [error] :as response} (parse-response response object)]
    (if error
      response
      (io/reader (:body response) :encoding "UTF-8"))))

(defn get-object
  [object]
  (let [req {:method     "GET"
             :uri        (str "/"
                              (get-in object [:s3 :bucket :name])
                              "/"
                              (get-in object [:s3 :object :key]))
             :headers    {"Host"                 "s3.eu-central-1.amazonaws.com"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (System/getenv "AWS_SESSION_TOKEN")}
             :service    "s3"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        common (common/authorize req)]

    (let [response (common/retry #(util/http-get
                                   (str "https://"
                                        (get (:headers req) "Host")
                                        (:uri req))
                                   {:as      :stream
                                    :headers (-> (:headers req)
                                                 (dissoc "Host")
                                                 (assoc "Authorization" common))
                                    :timeout 8000}
                                   :raw true)
                                 3)
          {:keys [error] :as response} (parse-response response object)]
      (if error
        response
        (io/reader (:body response) :encoding "UTF-8")))))

(defn delete-object
  [object]
  (let [req {:method     "DELETE"
             :uri        (str "/"
                              (get-in object [:s3 :bucket :name])
                              "/"
                              (get-in object [:s3 :object :key]))
             :headers    {"Host"                 "s3.eu-central-1.amazonaws.com"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (System/getenv "AWS_SESSION_TOKEN")}
             :service    "s3"
             :region     "eu-central-1"
             :access-key (System/getenv "AWS_ACCESS_KEY_ID")
             :secret-key (System/getenv "AWS_SECRET_ACCESS_KEY")}
        common (common/authorize req)]

    (let [response (common/retry #(util/http-delete
                                   (str "https://"
                                        (get (:headers req) "Host")
                                        (:uri req))
                                   {:as      :stream
                                    :headers (-> (:headers req)
                                                 (dissoc "host")
                                                 (assoc "Authorization" common))
                                    :timeout 8000}
                                   :raw true)
                                 3)]
      (when (contains? response :error)
        (log/error "Failed to fetch object" response))
      (if (> (:status response) 299)
        {:error (:body response)}
        {:body    (io/reader (:body response) :encoding "UTF-8")
         :headers (:headers response)}))))

(defn get-object-tagging
  [object]
  (let [req {:method     "GET"
             :uri        (str "/" (get-in object [:s3 :object :key]))
             :query      [["tagging" "True"]]
             :headers    {"Host"                 (str
                                                  (get-in object [:s3 :bucket :name])
                                                  ".s3.eu-central-1.amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (util/get-env "AWS_SESSION_TOKEN")}
             :service    "s3"
             :region     "eu-central-1"
             :access-key (util/get-env "AWS_ACCESS_KEY_ID")
             :secret-key (util/get-env "AWS_SECRET_ACCESS_KEY")}
        common (common/authorize req)]

    (let [response (common/retry #(util/http-get
                                   (str "https://"
                                        (get (:headers req) "Host")
                                        (:uri req))
                                   {:query-params (convert-query-params (:query req))
                                    :headers      (-> (:headers req)
                                                      (dissoc "host")
                                                      (assoc "Authorization" common))
                                    :timeout      8000}
                                   :raw true)
                                 3)]
      (when (contains? response :error)
        (log/error "Failed to fetch object" response))

      (->> (xml/parse (:body response))
           (:content)
           (first)
           (:content)
           (mapv
            (fn [{:keys [content]}]
              (let [key (first content)
                    val (second content)]
                (assoc
                 {}
                 (-> key
                     (:tag)
                     (name)
                     (str/lower-case)
                     (keyword))
                 (-> key
                     (:content)
                     (first))
                 (-> val
                     (:tag)
                     (name)
                     (str/lower-case)
                     (keyword))
                 (-> val
                     (:content)
                     (first))))))))))

(defn put-object-tagging
  [object tags]
  (let [tags (xml/emit-str
              {:tag     "Tagging"
               :content [{:tag     "TagSet"
                          :content (mapv
                                    (fn [{:keys [key value]}]
                                      {:tag     "Tag"
                                       :content [{:tag     "Key"
                                                  :content [key]}
                                                 {:tag     "Value"
                                                  :content [value]}]})
                                    tags)}]})
        req {:method     "PUT"
             :uri        (str "/" (get-in object [:s3 :object :key]))
             :query      [["tagging" "True"]]
             :headers    {"Host"                 (str
                                                  (get-in object [:s3 :bucket :name])
                                                  ".s3.eu-central-1.amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (util/get-env "AWS_SESSION_TOKEN")}
             :service    "s3"
             :region     "eu-central-1"
             :payload    tags
             :access-key (util/get-env "AWS_ACCESS_KEY_ID")
             :secret-key (util/get-env "AWS_SECRET_ACCESS_KEY")}
        common (common/authorize req)]
    (let [response (common/retry #(util/http-put
                                   (str "https://"
                                        (get (:headers req) "Host")
                                        (:uri req))
                                   {:query-params (convert-query-params (:query req))
                                    :body         (:payload req)
                                    :headers      (-> (:headers req)
                                                      (dissoc "host")
                                                      (assoc "Authorization" common))
                                    :timeout      8000}
                                   :raw true)
                                 3)]
      (when (contains? response :error)
        (log/error "Failed to fetch object" response))
      (if (> (:status response) 299)
        {:error (:body response)}
        {:version (get-in response [:headers :x-amz-version-id])}))))
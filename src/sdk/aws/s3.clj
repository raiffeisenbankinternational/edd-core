(ns sdk.aws.s3
  (:import
   java.time.OffsetDateTime
   java.time.ZoneOffset
   java.time.format.DateTimeFormatter)
  (:require
   [clj-aws-sign.core :as sign]
   [clojure.data.xml :as xml]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [lambda.http-client :as client]
   [lambda.util :as util]
   [ring.util.codec :as codec]
   [sdk.aws.common :as common]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn get-host
  [ctx]
  (str "s3."
       (get-in ctx [:aws :region])
       ".amazonaws.com"))

(defn convert-query-params
  [params]
  (reduce
   (fn [p [k v]]
     (assoc p k v))
   {}
   params))

(defn- check-what-is-missing
  [{:keys [body]}]
  (when body
    (let [body (xml/parse body)
          body {(:tag body) (:content body)}
          error (:Error body)
          error (reduce
                 (fn [p {:keys [tag content]}]
                   (assoc p
                          (-> tag
                              name
                              string/lower-case
                              keyword)
                          (first content)))
                 {}
                 error)]
      (if (= (:code error)
             "NoSuchKey")
        nil
        (throw (ex-info "Missing bucket"
                        error))))))

(defn- parse-response
  [response object]
  (log/debug "Auth response" response)
  (let [status (long (:status response 199))]
    (cond
      (contains? response :error) (do
                                    (log/warn "Failed update, client should handle error" response)
                                    {:error (:error response)})
      (= status 404) (check-what-is-missing response)
      (> status 299) (let [message (slurp (:body response))]
                       (log/warn "S3 Response failed"
                                 (:status response)
                                 message)
                       {:error {:status  (:status response)
                                :message message
                                :key     (get-in object [:s3 :object :key])
                                :bucket  (get-in object [:s3 :bucket :name])}})
      :else response)))

(defn get-aws-token [{:keys [aws]}]
  (let [token (:aws-session-token aws)]
    (if (empty? token)
      (System/getenv "AWS_SESSION_TOKEN")
      token)))

(defn s3-request-helper [{:keys [aws] :as ctx} object]
  {:headers    (merge {"Host"                 (get-host ctx)
                       "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                       "x-amz-date"           (common/create-date)
                       "x-amz-security-token" (get-aws-token ctx)}
                      (get-in object [:s3 :headers]))
   :service    "s3"
   :region     (:region aws)
   :access-key (:aws-access-key-id aws)
   :secret-key (:aws-secret-access-key aws)})

(defn put-object
  "puts object.content (should be plain string) into object.s3.bucket.name under object.s3.bucket.key"
  [{:keys [aws] :as ctx} object]
  (let [req
        (merge (s3-request-helper ctx object)
               {:method     "PUT"
                :uri        (str "/"
                                 (get-in object [:s3 :bucket :name])
                                 "/"
                                 (get-in object [:s3 :object :key]))})
        common (common/authorize req)
        response (client/retry-n #(-> (util/http-put
                                       (str "https://"
                                            (get (:headers req) "Host")
                                            (:uri req))
                                       (client/request->with-timeouts
                                        %
                                        {:as      :stream
                                         :headers (-> (:headers req)
                                                      (dissoc "Host")
                                                      (assoc "Authorization" common))
                                         :body    (get-in object [:s3 :object :content])})
                                       :raw true)
                                      (parse-response object)))
        {:keys [error] :as response} response]
    (if error
      response
      (io/reader (:body response) :encoding "UTF-8"))))

(defn enhanced-binary-stream
  [response]
  (let [nothing? (= "0" (:content-length (:headers response)))
        ^java.io.InputStream something (if nothing?
                                         nil
                                         (:body response))
        read-implementation
        (if nothing?
          (fn [_ _ _] -1)
          (fn
            ([] (.read something))
            ([^bytes b] (.read something b))
            ([^bytes b ^long offset ^long len] (.read something b (int offset) (int len)))))]
    (proxy [java.io.InputStream clojure.lang.IMeta] []
      (read
        ([] (read-implementation))
        ([^bytes b] (read-implementation b))
        ([^bytes b ^long offset ^long len] (read-implementation b offset len)))
      (meta [] {:empty-content? nothing?}))))

(defn get-object
  ([ctx object]
   (get-object ctx object {:retries client/retry-count}))
  ([{:keys [_aws] :as ctx}
    object
    {:keys [retries binary] :as _params}]
   (let [req
         (merge (s3-request-helper ctx object)
                {:method     "GET"
                 :uri        (str "/"
                                  (get-in object [:s3 :bucket :name])
                                  "/"
                                  (get-in object [:s3 :object :key]))})
         common (common/authorize req)
         response (client/retry-n #(-> (util/http-get
                                        (str "https://"
                                             (get (:headers req) "Host")
                                             (:uri req))
                                        (client/request->with-timeouts
                                         %
                                         {:as      :stream
                                          :headers (-> (:headers req)
                                                       (dissoc "Host")
                                                       (assoc "Authorization" common))})
                                        :raw true)
                                       (parse-response  object))
                                  :retries retries)
         {:keys [error] :as response} response]
     (if error
       response
       (when (:body response)
         (if binary
           (enhanced-binary-stream response)
           (io/reader (:body response) :encoding "UTF-8")))))))

(defn delete-object
  [{:keys [aws] :as ctx} object]
  (let [req
        (merge (s3-request-helper ctx object)
               {:method     "DELETE"
                :uri        (str "/"
                                 (get-in object [:s3 :bucket :name])
                                 "/"
                                 (get-in object [:s3 :object :key]))})

        common (common/authorize req)
        response (client/retry-n #(-> (util/http-delete
                                       (str "https://"
                                            (get (:headers req) "Host")
                                            (:uri req))
                                       (client/request->with-timeouts
                                        %
                                        {:as      :stream
                                         :headers (-> (:headers req)
                                                      (dissoc "host")
                                                      (assoc "Authorization" common))})
                                       :raw true)
                                      (parse-response object)))]
    (if (:error response)
      response
      {:body    (io/reader (:body response) :encoding "UTF-8")
       :headers (:headers response)})))

(defn get-object-tagging
  [{:keys [aws] :as ctx} object]
  (let [req {:method     "GET"
             :uri        (str "/" (get-in object [:s3 :object :key]))
             :query      [["tagging" "True"]]
             :headers    {"Host"                 (str
                                                  (get-in object [:s3 :bucket :name])
                                                  ".s3."
                                                  (:region aws)
                                                  ".amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (get-aws-token ctx)}
             :service    "s3"
             :region     (:region aws)
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        common (common/authorize req)]

    (let [response (client/retry-n #(-> (util/http-get
                                         (str "https://"
                                              (get (:headers req) "Host")
                                              (:uri req))
                                         (client/request->with-timeouts
                                          %
                                          {:as      :stream
                                           :query-params (convert-query-params (:query req))
                                           :headers      (-> (:headers req)
                                                             (dissoc "host")
                                                             (assoc "Authorization" common))})
                                         :raw true)
                                        (parse-response object)))]
      (if (or (nil? response)
              (:error response))
        response
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
                       (string/lower-case)
                       (keyword))
                   (-> key
                       (:content)
                       (first))
                   (-> val
                       (:tag)
                       (name)
                       (string/lower-case)
                       (keyword))
                   (-> val
                       (:content)
                       (first)))))))))))

(defn put-object-tagging
  [{:keys [aws] :as ctx} {:keys [object tags]}]
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
                                                  ".s3."
                                                  (:region aws)
                                                  ".amazonaws.com")
                          "Content-Type"         "application/json"
                          "Accept"               "application/json"
                          "x-amz-content-sha256" "UNSIGNED-PAYLOAD"
                          "x-amz-date"           (common/create-date)
                          "x-amz-security-token" (get-aws-token ctx)}
             :service    "s3"
             :region     (:region aws)
             :payload    tags
             :access-key (:aws-access-key-id aws)
             :secret-key (:aws-secret-access-key aws)}
        common (common/authorize req)]
    (let [response (client/retry-n #(-> (util/http-put
                                         (str "https://"
                                              (get (:headers req) "Host")
                                              (:uri req))
                                         (client/request->with-timeouts
                                          %
                                          {:as      :stream
                                           :query-params (convert-query-params (:query req))
                                           :body         (:payload req)
                                           :headers      (-> (:headers req)
                                                             (dissoc "host")
                                                             (assoc "Authorization" common))})
                                         :raw true)
                                        (parse-response object)))]
      (if (:error response)
        response
        {:version (get-in response [:headers :x-amz-version-id])}))))

(defn concat-content [a b]
  (flatten
   (conj [a] b)))

(defn xml-to-edn [xml]
  (if (some? (:tag xml))
    {(-> xml
         :tag
         name
         string/lower-case
         keyword)
     (let [m
           (mapv
            (fn [x] (xml-to-edn x))
            (:content xml))]
       (if (and (= 1 (count m))
                (string? (first m)))
         (first m)
         (apply merge-with concat-content m)))}

    xml))

(defn sort-contents [contents]
  (if (map? contents)
    [contents]
    (sort-by :lastmodified contents)))

(defn list-objects
  ([ctx object]
   (list-objects ctx object {:retries client/retry-count}))

  ([{:keys [_aws] :as ctx}
    {:keys [s3] :as object}
    {:keys [retries] :as _params}]

   (let [bucket (get-in s3 [:bucket :name])
         prefix (get-in s3 [:prefix])
         req
         (merge (s3-request-helper ctx object)
                {:method     "GET"
                 :uri        (str "/"
                                  bucket)
                 :query      [["list-type" "2"]
                              ["prefix" prefix]]})

         common (common/authorize req)
         url (str "https://"
                  (get (:headers req) "Host")
                  (:uri req)
                  "?list-type=2"
                  "&prefix=" prefix)
         response (client/retry-n #(-> (util/http-get
                                        url
                                        (client/request->with-timeouts
                                         %
                                         {:as      :stream
                                          :headers (-> (:headers req)
                                                       (dissoc "Host")
                                                       (assoc "Authorization" common)
                                                       (assoc "Accept" "application/json"))})
                                        :raw true)
                                       (parse-response  object))
                                  :retries retries)
         {:keys [error] :as response} response]
     (if error
       response
       (when (:body response)
         (-> (:body response)
             (io/reader :encoding "UTF-8")
             xml/parse
             xml-to-edn

             (update-in
              [:listbucketresult :contents]
              sort-contents)
             (doto tap>)))))))

(def ^DateTimeFormatter aws-formatter
  (-> "yyyyMMdd'T'HHmmss'Z'"
      (DateTimeFormatter/ofPattern)
      (.withZone ZoneOffset/UTC)))

(defn get-aws-timestamp ^String []
  (.format aws-formatter (OffsetDateTime/now)))

(defn presign-url
  "
  https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html

  Create a presigned, time-sensitive URL for a certain S3 object.
  When the method is :get (which is default), the URL is used to
  download a private file. Use the :post method to create a URL
  to upload a file directly to S3 from a browser through the standard
  <form> tag and a single file input.
  "
  (^String [aws bucket path expires-sec]
   (presign-url aws bucket path expires-sec nil))

  (^String [aws bucket path expires-sec {:keys [method]
                                         :or {method :get}}]

   (let [{:keys [region
                 aws-access-key-id
                 aws-secret-access-key
                 aws-session-token]}
         aws

         timestamp-full
         (get-aws-timestamp)

         timestamp-short
         (subs timestamp-full 0 8)

         credential
         (format "%s/%s/%s/s3/aws4_request"
                 aws-access-key-id
                 timestamp-short
                 region)

         host
         (format "%s.s3.%s.amazonaws.com" bucket region)

         headers
         {"host" host}

         query-params
         {"X-Amz-Algorithm" "AWS4-HMAC-SHA256"
          "X-Amz-Credential" credential
          "X-Amz-Date" timestamp-full
          "X-Amz-Expires" (str expires-sec)
          "X-Amz-SignedHeaders" "host"
          "X-Amz-Security-Token" aws-session-token}

         method-norm
         (-> method name string/upper-case)

         string-to-sign
         (sign/string-to-sign {:timestamp timestamp-full
                               :method method-norm
                               :uri path
                               :query query-params
                               :payload sign/UNSIGNED_PAYLOD
                               :short-timestamp timestamp-short
                               :region region
                               :service "s3"
                               :headers headers})

         signature
         (sign/signature {:secret-key aws-secret-access-key
                          :short-timestamp timestamp-short
                          :region region
                          :service "s3"
                          :string-to-sign string-to-sign})

         base-url
         (format "https://%s/%s" host path)]

     (format "%s?%s"
             base-url
             (-> query-params
                 (assoc "X-Amz-Signature" signature)
                 (codec/form-encode))))))

(comment

  ;; generate temporary creds in CloudShell
  (def AWS
    {:aws-access-key-id ""
     :aws-secret-access-key ""
     :aws-session-token ""
     :region "eu-central-1"})

  (presign-url AWS
               "118123141711-dev19-daily-calculation-batch"
               "reports/2024-03-25/glms-application-journal-report.csv"
               3600)

  nil)

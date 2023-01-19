(ns sdk.aws.s3-it
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [sdk.aws.s3 :as s3]
            [lambda.uuid :as uuid]
            [lambda.util :as util]
            [aws.lambda :as lambda]))

(defn for-object
  [key]
  {:s3 {:bucket {:name (str
                        (util/get-env
                         "AccountId")
                        "-"
                        (util/get-env
                         "EnvironmentNameLower")
                        "-it")}
        :object {:key key}}})

(defn for-object-with-content
  [key content]
  {:s3 {:bucket {:name (str
                        (util/get-env
                         "AccountId")
                        "-"
                        (util/get-env
                         "EnvironmentNameLower")
                        "-it")}
        :object  {:key key
                  :content content}}})

(defn gen-key
  []
  (let [key (str (uuid/gen))]
    (log/info "Generation s3 key: " key)
    key))

(deftest test-s3-upload
  (let [ctx    {:aws (lambda/fetch-aws-config)
                :service-name (keyword (util/get-env
                                        "ServiceName"
                                        "local-test"))
                :hosted-zone-name (util/get-env
                                   "PublicHostedZoneNetme"
                                   "example.com")
                :environment-name-lower (util/get-env
                                         "EnvironmentNameLower")}]
    (testing "Testing happy path of put and get object"
      (let [key (gen-key)
            data "sample-data"]
        (s3/put-object ctx (for-object-with-content key data))
        (is (= data
               (slurp
                (s3/get-object ctx (for-object-with-content key "sample-data")))))))

    (testing "Delete getting missing object"
      (let [key (gen-key)
            object (for-object key)]
        (is (= nil
               (s3/get-object ctx object)))))

    (testing "Testing when there is error putting object to s3"
      (let [key (gen-key)
            data "sample-data"
            atempt (atom 0)
            original-function util/http-put]
        (with-redefs [util/http-put (fn [url request & {:keys [raw]}]
                                      (log/info "Attempt: " @atempt)
                                      (swap! atempt inc)
                                      (if (< @atempt 2)
                                        {:body (char-array data)
                                         :status 503}
                                        (original-function url request :raw raw)))]
          (s3/put-object ctx (for-object-with-content key data)))
        (is (= 2
               @atempt))
        (is (= data
               (slurp
                (s3/get-object ctx (for-object-with-content key "sample-data")))))))

    (testing "Testing when there is error getting-object-from s3"
      (let [key (gen-key)
            data "sample-data"
            atempt (atom 0)
            original-function util/http-get]
        (with-redefs [util/http-get (fn [url request & {:keys [raw]}]
                                      (log/info "Attempt: " @atempt)
                                      (swap! atempt inc)
                                      (if (< @atempt 2)
                                        {:body (char-array data)
                                         :status 503}
                                        (original-function url request :raw raw)))]
          (s3/put-object ctx (for-object-with-content key data))
          (let [resp (s3/get-object ctx (for-object-with-content key "sample-data"))]
            (is (= 2
                   @atempt))
            (is (= data
                   (slurp resp)))))))))

(deftest test-s3-tagging
  (let [ctx    {:aws (lambda/fetch-aws-config)
                :service-name (keyword (util/get-env
                                        "ServiceName"
                                        "local-test"))
                :hosted-zone-name (util/get-env
                                   "PublicHostedZoneNetme"
                                   "example.com")
                :environment-name-lower (util/get-env
                                         "EnvironmentNameLower")}]
    (testing "Testing happy path of put and get object tags"
      (let [key (gen-key)
            data "sample-data"
            object (for-object key)
            tags [{:key "testkey"
                   :value "testvalue"}]]

        (s3/put-object ctx (assoc-in object [:s3 :object :content] data))
        (s3/put-object-tagging ctx {:object object
                                    :tags tags})
        (is (= tags
               (s3/get-object-tagging ctx object)))))

    (testing "Delete getting missing tagged"
      (let [key (gen-key)
            object (for-object key)]
        (is (= nil
               (s3/get-object-tagging ctx object)))))

    (testing "Testing when there is error putting tags"
      (let [key (gen-key)
            data "sample-data"
            object (for-object key)
            tags [{:key "testkey"
                   :value "testvalue"}]
            atempt (atom 0)
            original-function util/http-put]

        (s3/put-object ctx (assoc-in object [:s3 :object :content] data))
        (with-redefs [util/http-put (fn [url request & {:keys [raw]}]
                                      (log/info "Attempt: " @atempt)
                                      (swap! atempt inc)
                                      (if (< @atempt 2)
                                        {:body (char-array data)
                                         :status 503}
                                        (original-function url request :raw raw)))]
          (s3/put-object-tagging ctx {:object object
                                      :tags tags})
          (is (= 2
                 @atempt))
          (is (= tags
                 (s3/get-object-tagging ctx object))))))

    (testing "Testing when there is error getting tags"
      (let [key (gen-key)
            data "sample-data"
            object (for-object key)
            tags [{:key "testkey"
                   :value "testvalue"}]
            atempt (atom 0)
            original-function util/http-get]

        (s3/put-object ctx (assoc-in object [:s3 :object :content] data))
        (s3/put-object-tagging ctx {:object object
                                    :tags tags})
        (with-redefs [util/http-get (fn [url request & {:keys [raw]}]
                                      (log/info "Attempt: " @atempt)
                                      (swap! atempt inc)
                                      (if (< @atempt 2)
                                        {:body (char-array data)
                                         :status 503}
                                        (original-function url request :raw raw)))]

          (let [response (s3/get-object-tagging ctx object)]
            (is (= 2
                   @atempt))
            (is (= tags
                   response))))))))

(deftest test-s3-delete
  (let [ctx    {:aws (lambda/fetch-aws-config)
                :service-name (keyword (util/get-env
                                        "ServiceName"
                                        "local-test"))
                :hosted-zone-name (util/get-env
                                   "PublicHostedZoneNetme"
                                   "example.com")
                :environment-name-lower (util/get-env
                                         "EnvironmentNameLower")}]
    (testing "Testing happy path of put and delete"
      (let [key (gen-key)
            data "sample-data"
            object (for-object key)]

        (s3/put-object ctx (assoc-in object [:s3 :object :content] data))
        (is (= data
               (slurp (s3/get-object ctx object))))
        (s3/delete-object ctx object)
        (is (= nil
               (s3/get-object ctx object)))))

    (testing "Delete missing object"
      (let [key (gen-key)
            object (for-object key)]
        (is (= nil
               (s3/get-object ctx object)))))

    (testing "Testing when there is error deleteing"
      (let [key (gen-key)
            data "sample-data"
            object (for-object key)
            atempt (atom 0)
            original-function util/http-delete]

        (s3/put-object ctx (assoc-in object [:s3 :object :content] data))
        (is (= data
               (slurp (s3/get-object ctx object))))
        (with-redefs [util/http-delete (fn [url request & {:keys [raw]}]
                                         (log/info "Attempt: " @atempt)
                                         (swap! atempt inc)
                                         (if (< @atempt 2)
                                           {:body (char-array data)
                                            :status 503}
                                           (original-function url request :raw raw)))]
          (s3/delete-object ctx object)
          (is (= 2
                 @atempt))
          (is (= nil
                 (s3/get-object ctx object))))))))

(deftest test-s3-missing-things
  (let [ctx    {:aws (lambda/fetch-aws-config)
                :service-name (keyword (util/get-env
                                        "ServiceName"
                                        "local-test"))
                :hosted-zone-name (util/get-env
                                   "PublicHostedZoneNetme"
                                   "example.com")
                :environment-name-lower (util/get-env
                                         "EnvironmentNameLower")}
        gen-bucket (fn []
                     (str
                      (util/get-env
                       "AccountId")
                      "-"
                      (util/get-env
                       "EnvironmentNameLower")
                      "-"
                      (uuid/gen)))
        gen-object (fn [bucket]
                     {:s3 {:bucket {:name bucket}
                           :object {:key (->> (uuid/gen)
                                              (str "prefix/"))}}})]

    (let [bucket (gen-bucket)
          object (gen-object bucket)]
      (try
        (s3/delete-object ctx  object)
        (throw (ex-info "Should not come here"
                        {:expected "Exception"}))
        (catch Exception e
          (is (= {:code "NoSuchBucket",
                  :message "The specified bucket does not exist",
                  :bucketname bucket}
                 (-> (ex-data e)
                     (select-keys [:code :message :bucketname])))))))
    (let [bucket (gen-bucket)
          object (assoc-in (gen-object bucket)
                           [:s3 :object :content] "Test")]
      (try
        (s3/put-object ctx  object)
        (throw (ex-info "Should not come here"
                        {:expected "Exception"}))
        (catch Exception e
          (is (= {:code "NoSuchBucket",
                  :message "The specified bucket does not exist",
                  :bucketname bucket}
                 (-> (ex-data e)
                     (select-keys [:code :message :bucketname])))))))
    (let [bucket (gen-bucket)
          object (gen-object bucket)]
      (try
        (s3/get-object ctx  object)
        (throw (ex-info "Should not come here"
                        {:expected "Exception"}))
        (catch Exception e
          (is (= {:code "NoSuchBucket",
                  :message "The specified bucket does not exist",
                  :bucketname bucket}
                 (-> (ex-data e)
                     (select-keys [:code :message :bucketname])))))))))


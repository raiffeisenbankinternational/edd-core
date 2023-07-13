(ns sdk.aws.s3-test
  (:require
   [sdk.aws.s3 :as s3]
   [clojure.test :refer [deftest is]]))

(deftest test-xml-to-edn
  (is (=
       {:listbucketresult
        {:name "dev08-plc3",
         :prefix "current",
         :keycount "3",
         :maxkeys "1000",
         :istruncated "false",
         :contents
         [{:key "current/",
           :lastmodified "2023-07-12T13:15:58.000Z",
           :etag "\"d41d8cd98f00b204e9800998ecf8427e\"",
           :size "0",
           :storageclass "STANDARD"}
          {:key "current/test.txt",
           :lastmodified "2023-07-12T13:16:53.000Z",
           :etag "\"5d41402abc4b2a76b9719d911017c592\"",
           :size "5",
           :storageclass "STANDARD"}
          {:key "current/test2.txt",
           :lastmodified "2023-07-12T15:16:56.000Z",
           :etag "\"5d41402abc4b2a76b9719d911017c592\"",
           :size "5",
           :storageclass "STANDARD"}]}}

       (s3/xml-to-edn
        #xml/element
         {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/ListBucketResult,
          :content [#xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Name,
                      :content ["dev08-plc3"]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Prefix,
                      :content ["current"]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/KeyCount,
                      :content ["3"]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/MaxKeys,
                      :content ["1000"]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/IsTruncated,
                      :content ["false"]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Contents,
                      :content [#xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Key,
                                  :content ["current/"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/LastModified,
                                  :content ["2023-07-12T13:15:58.000Z"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/ETag,
                                  :content ["\"d41d8cd98f00b204e9800998ecf8427e\""]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Size,
                                  :content ["0"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/StorageClass,
                                  :content ["STANDARD"]}]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Contents,
                      :content [#xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Key,
                                  :content ["current/test.txt"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/LastModified,
                                  :content ["2023-07-12T13:16:53.000Z"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/ETag,
                                  :content ["\"5d41402abc4b2a76b9719d911017c592\""]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Size,
                                  :content ["5"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/StorageClass,
                                  :content ["STANDARD"]}]}
                    #xml/element
                     {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Contents,
                      :content [#xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Key,
                                  :content ["current/test2.txt"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/LastModified,
                                  :content ["2023-07-12T15:16:56.000Z"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/ETag,
                                  :content ["\"5d41402abc4b2a76b9719d911017c592\""]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/Size,
                                  :content ["5"]}
                                #xml/element
                                 {:tag :xmlns.http%3A%2F%2Fs3.amazonaws.com%2Fdoc%2F2006-03-01%2F/StorageClass,
                                  :content ["STANDARD"]}]}]}))))

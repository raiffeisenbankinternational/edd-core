(ns edd.io.core-test
  (:import
   (java.io File
            ByteArrayInputStream
            ByteArrayOutputStream))
  (:require
   [clojure.test :refer [deftest is]]
   [edd.io.core :as io]))

(deftest test-edn-ok

  (let [file
        (io/get-temp-file "foo" "bar")

        _
        (spit file (pr-str {:foo 42}))

        data
        (io/read-edn file)]

    (is (= {:foo 42} data))))

(deftest test-gzip-streams-ok

  (let [in
        (-> "hello"
            (.getBytes)
            (ByteArrayInputStream.))

        file
        (io/write-temp-file-gzip in)

        size
        (io/file-size file)

        data
        (-> file
            (io/gzip-input-stream)
            slurp)]

    (is (= 25 size))
    (is (= "hello" data)))

  (let [in
        (-> "hello"
            (.getBytes)
            (ByteArrayInputStream.))

        file
        (io/write-temp-file in)

        size
        (io/file-size file)

        data
        (-> file
            slurp)]

    (is (= 5 size))
    (is (= "hello" data))))

(deftest test-file-exists?
  (let [capture! (atom nil)]
    (io/with-tmp-file [file]
      (reset! capture! file)
      (is (io/file-exists? file))
      (is (not (io/is-directory? file))))
    (is (not (io/file-exists? @capture!)))))

(deftest test-read-bytes
  (let [file
        (io/get-temp-file "foo" "bar")]
    (spit file "hello")
    (is (= [104 101 108 108 111]
           (-> file io/read-bytes vec)))))

(deftest test-with-pipe
  (let [fut
        (io/with-pipe [o i]
          (let [fut
                (future
                  (with-open [in-gzip (io/gzip-input-stream i)]
                    (.readAllBytes in-gzip)))]
            (with-open [out-gzip (io/gzip-output-stream o)]
              (.write out-gzip (.getBytes "hello world")))
            fut))]
    (is (future? fut))
    (is (= "hello world"
           (-> fut deref (String.))))))

(deftest test-io-proxies

  (with-open [in (io/input-stream (.getBytes "abc"))]
    (is (io/input-stream? in))
    (is (= "abc"
           (slurp in))))

  (with-open [r (io/reader (.getBytes "abc"))]
    (is (io/reader? r))
    (is (= "abc"
           (slurp r))))

  (let [baos (new ByteArrayOutputStream)]
    (with-open [out (io/output-stream baos)]
      (is (io/output-stream? out))
      (.write out (.getBytes "abc")))
    (is (= "abc"
           (new String (.toByteArray baos)))))

  (let [baos (new ByteArrayOutputStream)]
    (with-open [w (io/writer baos)]
      (is (io/writer? w))
      (.write w "abc"))
    (is (= "abc"
           (new String (.toByteArray baos)))))

  (is (io/file? (io/file "a")))
  (is (io/file? (io/file "a" "b")))
  (is (io/file? (io/file "a" "b" "c" "d")))
  (is (= "a/b/c/d"
         (str (io/file "a" "b" "c" "d")))))

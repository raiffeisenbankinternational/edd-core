(ns edd.io.core-test
  (:import
   java.io.ByteArrayInputStream)
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
        (io/stream->gzip-temp-file in)

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
        (io/stream->temp-file in)

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

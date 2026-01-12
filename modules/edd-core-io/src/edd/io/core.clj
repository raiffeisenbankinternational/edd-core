(ns edd.io.core
  "
  Basic IO utilities for streams, files, GZIP encoding
  and decoding, MIME detection, and similar things.
  The goal is to not depend on the clojure.java.io
  namespace.
  "
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import (java.io File
                    PipedInputStream
                    PipedOutputStream
                    PushbackReader
                    ByteArrayOutputStream
                    Reader
                    Writer
                    InputStream
                    OutputStream)
           (java.net URL)
           (java.nio.file Files)
           (java.util.zip GZIPInputStream
                          GZIPOutputStream)))

(set! *warn-on-reflection* true)

(defn resource!
  "
  Get a resource throwing an exception when it's missing.
  "
  ^URL [^String path]
  (or (io/resource path)
      (throw (new RuntimeException
                  (format "missing resource: %s" path)))))

(defn reader
  "
  A proxy to the standard io/reader function.
  "
  (^Reader [src]
   (io/reader src))
  (^Reader [src & opts]
   (apply io/reader src opts)))

(defn input-stream
  "
  A proxy to the standard io/input-stream function.
  "
  (^InputStream [src]
   (io/input-stream src))
  (^InputStream [src & opts]
   (apply io/input-stream src opts)))

(defn writer
  "
  A proxy to the standard io/writer function.
  "
  (^Writer [src]
   (io/writer src))
  (^Writer [src & opts]
   (apply io/writer src opts)))

(defn file
  "
  A proxy to the standard io/file function.
  "
  (^File [arg]
   (io/file arg))
  (^File [parent child]
   (io/file parent child))
  (^File [parent child & more]
   (apply io/file parent child more)))

(defn output-stream
  "
  A proxy to the standard io/output-stream function.
  "
  (^OutputStream [src]
   (io/output-stream src))
  (^OutputStream [src & opts]
   (apply io/output-stream src opts)))

(defn pushback-reader ^PushbackReader [src]
  (new PushbackReader src))

(defn read-edn
  "
  Like `clojure.edn/read` but wraps the input into
  the pushback reader under the hood. The `src` is
  anything which can be converted into an input-stream.
  "
  ([src]
   (read-edn src {}))

  ([src opt]
   (with-open [in (-> src
                      (io/reader)
                      (pushback-reader))]
     (edn/read opt in))))

(defn byte-array-output-stream
  "
  Create a new ByteArrayOutputStream instance.
  "
  (^ByteArrayOutputStream []
   (new ByteArrayOutputStream))
  (^ByteArrayOutputStream [size]
   (new ByteArrayOutputStream size)))

(defn get-temp-file
  "
  Return an temporal file, an instance of java.io.File class.
  "
  (^File []
   (get-temp-file "tmp" ".tmp"))
  (^File [prefix suffix]
   (File/createTempFile prefix suffix)))

(defmacro with-tmp-file
  "
  Execute the body while the `bind` symbol is bound
  to a temporal file. Afterwards, the file is deleted.
  "
  [[bind prefix suffix] & body]
  `(let [~bind (get-temp-file (or ~prefix "tmp")
                              (or ~suffix ".tmp"))]
     (try
       ~@body
       (finally
         (.delete ~bind)))))

(defn gzip-output-stream
  "
  Coerce the source into the GZIPOutputStream instance.
  "
  ^GZIPOutputStream [src]
  (-> src
      (io/output-stream)
      (GZIPOutputStream.)))

(defn gzip-input-stream
  "
  Coerce the source into the GZIPInputStream instance.
  "
  ^GZIPInputStream [src]
  (-> src
      (io/input-stream)
      (GZIPInputStream.)))

(defn write-file
  "
  Write src into a given File instance.
  "
  ^File [src ^File file]
  (with-open [in (io/input-stream src)
              out (io/output-stream file)]
    (.transferTo in out))
  file)

(defn write-temp-file
  "
  Write src into temp file and return it.
  "
  (^File [src]
   (let [tmp (get-temp-file)]
     (write-file src tmp)))

  (^File [src prefix suffix]
   (let [tmp (get-temp-file prefix suffix)]
     (write-file src tmp))))

(defn write-temp-file-gzip
  "
  Write src into a gzipped temp file and return it.
  "
  ^File [src]
  (let [file (get-temp-file)]
    (with-open [in
                (io/input-stream src)

                out
                (->> file
                     io/output-stream
                     (new GZIPOutputStream))]
      (.transferTo in out)
      (.finish out))
    file))

(defn gzip-reader ^Reader [src]
  (-> src
      io/input-stream
      GZIPInputStream.
      io/reader))

(defn file->extension
  "
  Given a file instance, return if extension detected
  as everything after the last dot.
  "
  ^String [^File file]
  (some-> file
          (str)
          (str/split #"\.")
          (last)))

(defn file->mime-type
  "
  Given a file instance, guess its MIME type by an extension.
  Only basic types are supported. Default is application/octet-stream.
  "
  ^String [^File file]
  (case (-> file file->extension str/lower-case)
    ("jpg" "jpeg") "image/jpeg"
    "png"          "image/png"
    "csv"          "text/csv"
    "xlsx"         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    "xls"          "application/vnd.ms-excel"
    "doc"          "application/msword"
    "docx"         "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    "txt"          "text/plain"
    "pdf"          "application/pdf"
    ("gz" "gzip")  "application/gzip"
    "xml"          "application/xml"
    "json"         "application/json"
    "nippy"        "application/nippy"
    "application/octet-stream"))

(defn file-size
  "
  Return the file size in bytes as Long.
  "
  ^Long [^File file]
  (Files/size (.toPath file)))

(defn file-exists?
  "
  Whether the file exists or not.
  "
  ^Boolean [^File file]
  (.exists file))

(defn is-directory?
  "
  Whether the file is a directory or not
  "
  ^Boolean [^File file]
  (.isDirectory file))

(defn file->name
  "
  Return the basic file name.
  "
  ^String [^File file]
  (.getName file))

(defn delete-file
  "
  Physically delete a file.
  "
  [^File file]
  (.delete file))

(defn read-bytes
  "
  Read all bytes from a source into a byte array.
  "
  ^bytes [src]
  (with-open [in (io/input-stream src)]
    (.readAllBytes in)))

(defn file? [src]
  (instance? File src))

(defn output-stream? [src]
  (instance? OutputStream src))

(defn input-stream? [src]
  (instance? InputStream src))

(defn reader? [src]
  (instance? Reader src))

(defn writer? [src]
  (instance? Writer src))

(defmacro with-pipe
  "
  A macro to manage piped writing/reading across two threads.
  The idea is to write bytes into an output but not exhaust
  disk and memory, while another thread is consuming the
  connected input stream.

  - `out` is bound to a new instance of PipedOutputStream
  - `in` is bound to a new instance of PipedInputStream
  - `size` is an optional size for the in.

  The in is connected to the out meaning everything written
  to the out is available from in again. The out gets closed
  when exiting the macro. The in does not because it is used
  in another thread. You spawn a future that reads from
  the in, write anything you want to the out, and return
  the future. Example:

  (with-pipe [o i 0xFF]
    (let [fut (future
                (.readAllBytes i))]
      (doseq [x (range 0 3)]
        (.write o x))
      fut))

  Both out and in can be wrapped with Gzip in/output streams,
  writers/readers, etc.
  "
  [[out in size] & body]
  `(with-open [~out (new PipedOutputStream)]
     (let [~in (new PipedInputStream ~@(when size [size]))]
       (.connect ~out ~in)
       ~@body)))

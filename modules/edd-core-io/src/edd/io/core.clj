(ns edd.io.core
  "
  Basic IO utilities for streams and files.
  "
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:import java.io.File
           java.io.InputStream
           java.io.PipedInputStream
           java.io.PipedOutputStream
           java.io.PushbackReader
           java.io.Reader
           java.nio.file.Files
           java.util.zip.GZIPInputStream
           java.util.zip.GZIPOutputStream))

(set! *warn-on-reflection* true)

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

(defn stream->file
  "
  Dump a stream in to a file. Return a number of
  bytes transferred as Long.
  "
  ^Long [^InputStream input-stream
         ^File file]
  (with-open [in input-stream
              out (io/output-stream file)]
    (.transferTo in out)))

(defn stream->temp-file
  "
  Create a temporal file and transfer the stream
  into it. Return the file instance.
  "
  (^File [^InputStream input-stream]
   (let [tmp (get-temp-file)]
     (stream->file input-stream tmp)
     tmp))

  (^File [^InputStream input-stream prefix suffix]
   (let [tmp (get-temp-file prefix suffix)]
     (stream->file input-stream tmp)
     tmp)))

(defn stream->gzip-temp-file
  "
  Like stream->temp-file but additionally compress
  the payload with Gzip.
  "
  ^File [^InputStream input-stream]

  (let [tmp (get-temp-file)]
    (with-open [in
                input-stream

                out
                (-> tmp
                    (io/output-stream)
                    (gzip-output-stream))]

      (.transferTo in out))

    tmp))

(defn file->gzip-stream
  "
  Turn a Gzip-compressed file into a stream.
  "
  ^InputStream [^File file]
  (-> file
      (io/input-stream)
      (gzip-input-stream)))

(defn file->gzip-reader
  "
  Turn a Gzip-compressed file into a reader.
  "
  ^Reader [^File file]
  (-> file
      (io/input-stream)
      (gzip-input-stream)
      (io/reader)))

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

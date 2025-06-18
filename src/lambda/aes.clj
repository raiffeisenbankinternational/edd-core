(ns lambda.aes
  (:require [clojure.test :refer :all])
  (:import (java.security Key)
           (javax.crypto Cipher SecretKeyFactory)
           (javax.crypto.spec SecretKeySpec PBEKeySpec IvParameterSpec)
           (java.util Base64 Arrays)))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- ^Key secret-spec
  [^bytes msg-bytes, ^String password]
  (let [key-factory (SecretKeyFactory/getInstance "PBKDF2WithHmacSHA256")
        salt (Arrays/copyOfRange msg-bytes 8 16)
        key-spec (PBEKeySpec. (.toCharArray password)
                              salt
                              65536
                              256)
        generated-key (.generateSecret key-factory key-spec)]
    (SecretKeySpec. (.getEncoded generated-key) "AES")))

(defn ^String decrypt
  [^String msg ^String password ^String iv]
  (let [msg-bytes (.decode
                   (Base64/getDecoder)
                   msg)
        iv (IvParameterSpec. (.decode
                              (Base64/getDecoder)
                              iv))
        cipher (doto (Cipher/getInstance "AES/CBC/PKCS5Padding")
                 (.init Cipher/DECRYPT_MODE
                        (secret-spec msg-bytes password)
                        iv))
        bytes (.doFinal cipher (Arrays/copyOfRange msg-bytes 16 (alength msg-bytes)))]
    (String. bytes "UTF-8")))


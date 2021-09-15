(ns lambda.jwt-test
  (:require [clojure.test :refer :all]
            [lambda.jwt :as jwt]
            [lambda.util :as util]
            [lambda.util-test :as util-test]
            [lambda.util :as util]))

(def token "eyJraWQiOiJTOWNDcTR5SVwvTW83cThhc241ZlFtaVFcL0k5YklrUTI5cmtrb1wvNTJmQmVnPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibnFRZ2ViM29USFFhYTEtMEtOY0stdyIsInN1YiI6IjdhMTE4MmY4LTFhN2UtNGY5YS05MGI0LTVmMmIwZWYyNGQzNyIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTMiLCJncm91cC0yIiwiZ3JvdXAtMSJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tXC9ldS1jZW50cmFsLTFfQUNYWXVsMDBRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGhAZXhhbXBsZS5jb20iLCJhdWQiOiI1ZmI1NTg0M2YxZG9qM2hqbHE3b25nZm1uMSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDcyOTc1LCJleHAiOjE2MzI0NzY1NzUsImlhdCI6MTYzMjQ3Mjk3NSwianRpIjoiYTg1NDUwZTMtYmMxOS00ODU5LTllOWMtMjY0NDZkMmFmYjMxIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.yFvBZZoSfAkAv6hCJ_XUiTJH8xi-SavQoNLWiw_BjKZkkjQdrapGem5GdIXi02oBbhNcLNKGbKMSn9SlQ4BvQxWalzeynhY703lH2d0yg1sAeSViGPC3Gh6TRstrLjyNYOL7A-qIM1m0HcT9ByyDUmTpgeXZx8itWMTHWb7KCQPTZc1Uza4rnHvwyg6Lnsu9kGIYYIqReyk2QuG9ggehdZ39AS3O152tfRMOgtLJytwwtjDj1kmDclKKQj8gnplq9bu3JORv1ull37M-Vm-gAg_XKbL3_acah1XoXOeekLhXC7jE6J5lNnZdKaAyN6uC6ojpOCFjAAEho8_yfGOsag")
(def token-invalid-signature "eyJraWQiOiJTOWNDcTR5SVwvTW83cThhc241ZlFtaVFcL0k5YklrUTI5cmtrb1wvNTJmQmVnPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibnFRZ2ViM29USFFhYTEtMEtOY0stdyIsInN1YiI6IjdhMTE4MmY4LTFhN2UtNGY5YS05MGI0LTVmMmIwZWYyNGQzNyIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTMiLCJncm91cC0yIiwiZ3JvdXAtMSJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tXC9ldS1jZW50cmFsLTFfQUNYWXVsMDBRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGhAZXhhbXBsZS5jb20iLCJhdWQiOiI1ZmI1NTg0M2YxZG9qM2hqbHE3b25nZm1uMSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDcyOTc1LCJleHAiOjE2MzI0NzY1NzUsImlhdCI6MTYzMjQ3Mjk3NSwianRpIjoiYTg1NDUwZTMtYmMxOS00ODU5LTllOWMtMjY0NDZkMmFmYjMxIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.yFvBZZoSfAkAv6hCJ_XUiTJH8xi-SavQoNLWiw_BjKZkkjQdrapGem5GdIXi02oBbhNcLNKGbKMSn9SlQ4BvQxWalzeynhY703lH2d0yg1sAeSViGPC3Gh6TRstrLjyNYOL7A-qIM1m0HcT9ByyDUmTpgeXZx8itWMTHWb7KCQPTZc1Uza4rnHvwyg6Lnsu9kGIYYIqReyk2QuG9ggehdZ39AS3O152tfRMOgtLJytwwtjDj1kmDclKKQj8gnplq9bu3JORv1ull37M-Vm-gAg_XKbL3_acah1XoXOeekLhXC7jE6J5lNnZdKaAyN6uC6ojpOCFjAAEho8_yfGOssg")
(def token-user-pool-2
  "eyJraWQiOiJIUXFsd0pJaHJcL0srYUhGSWRUVlRxNzF5ZUo1MFNrbm9Mb0s0ZEdcL0huOXc9IiwiYWxnIjoiUlMyNTYifQ.eyJhdF9oYXNoIjoiQlpDNG5DdWxtOGZaVHlxcXRNSVhvZyIsInN1YiI6ImM4ZTE3YzI3LTYyNzctNGEyZi1iMDYxLTkxMTgxMzZmN2Q4MiIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAuZXUtY2VudHJhbC0xLmFtYXpvbmF3cy5jb21cL2V1LWNlbnRyYWwtMV9mM2J4Y0ltOFkiLCJjb2duaXRvOnVzZXJuYW1lIjoiam9obi5zbWl0aEBleGFtcGxlLmNvbSIsImF1ZCI6Ijdwa3FscmwyN2NnaDE4NGVpOGYydWt0ZzgiLCJldmVudF9pZCI6ImE2NWMwNWJhLWU0NWYtNGFiNi04YTJmLWUwOTFlNTAxNDRhYSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDc2MjQwLCJleHAiOjE2MzI0Nzk4NDAsImlhdCI6MTYzMjQ3NjI0MCwianRpIjoiNmJiYmJjN2ItODI4ZS00OWJkLTg2ZDMtNWQwYjI3NjI0MmMwIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.YPcGoQbU40M1sWKNHSSaEb2Oa6OQKO66t8hrE8lpZG3IpT5h-hKIewFll0U8raO3t7qQP2OLwz3Q38REzMc9yfdLAwmLMEEKMrz3A0dLYO_vqoHInE9ptL97odeESNGtyLBJO0P5WhtZKT_RG-ss6tji0fP5z7G_0ly5E_ggYPb26c-5PfVbWffgIzg2kjNAcDG3zbD39vq5RP9XJfF-HnvPcLZxd0ZScQIaCDGv6kHDMZxIzPtMUo1bIwAgLr54hbANaSACYgwYYlorHO7kevt3X50NytztecczJF7AdBdsg6NIKx7rjgXjbXkM-PVG0pfB0mp-TDoIBZSvgjQMwg")

(def jwks-key {:keys
               [{:alg "RS256",
                 :e "AQAB",
                 :kid "S9cCq4yI/Mo7q8asn5fQmiQ/I9bIkQ29rkko/52fBeg=",
                 :kty "RSA",
                 :n
                 "zUo98BHmk5TO-XYflS_8r25nxxpPU6SyM4xba1CsBhfRF9OO9QEnPcaijz7XDNHuQmv0zcM-_uEDqN8OhuwCcXG2XtbONOO_qV4NArDd1t21z6HG0W9I6Ky89ajUWX2R48Y-lqQR-22qD_YHtFZ8TQTIj1ExWTzsWjYDbHRUIxU44Inhg2rJKwpDN937K02layjuiOZRIchPTb18cr9BCpzWHG4hQB3Pm-gvmZ_BEEoKF7iiwnV1RThF2DcLKG2hyoGF31gczdpySInjCVSDnLZomjCjWGyr5pEnygdq8dEIu39rCf9Wz2aLTSZAFZK95LwAIO0_K_UnSbSG5ldbXw",
                 :use "sig"}
                {:alg "RS256",
                 :e "AQAB",
                 :kid "drv6h8K7Vh4axnnJcD2161kJpBgzDMIP0T3ERzzpM1A=",
                 :kty "RSA",
                 :n
                 "uRQ-pJAKYoH8-0UiPMlJ-KEsAB-TT4hDu2gsgadVxaAlXz6z3hTeTV0aTaEpj30P39kwReVC7Dt9JJodmH6MmkMIfPRVwJh9dpwtUjrI3CG4-Ax2gKREibEMZPJez9dBaiz1cKaKVWHj-EU6f6K1rMvDKrVC3VSahEt50ZKii8dJPi1BoBandVfuisfejhOcLEbCuFV1MTBVz-jdV6q1rr4J-EeK7d41JIfRLAY9Dtv-jW_p1I6EpwCxQDMC8Px7GySW5TLhnSVo-Ff_9helWdXWYfAH8gEcdUFT8sW2wkoP5QXK19iLoGphrGCsg4nmmM_sAhYoYVj-7wjM5-vYAQ",
                 :use "sig"}]})

(def jwks-key-2 {:keys
                 [{:alg "RS256",
                   :e "AQAB",
                   :kid "HQqlwJIhr/K+aHFIdTVTq71yeJ50SknoLoK4dG/Hn9w=",
                   :kty "RSA",
                   :n
                   "pT6s1QhVtMdb_f_CTwtVTbYQuNXBVssn5vs9UIHurn7zE5_2iMMcHGCdzfcE1axj8N884XBr4NyKJ_LMedABg7rPqmVz-hfqXPkfA_32ikMU3VJtj6UYA75StBU6pkl2-OKAnctbQO05ElL4dRsAIPpKOpniX5-c5Gb_UgXOmxsAc2zQ61aQUhIJ10tTSjwU2DmXaPaFMJXQyZUZ0TiRUSrOSqJlCLmY9juWtIoranrIN4YqUfelXtLuxMc6PKBpM79nULedd_7FF720puUGasx54kupjGn_-XVqM1rgAR9EqU2TAITro6QljTAZT5L2h8HVkwbi0S_570drCkqcIw",
                   :use "sig"}
                  {:alg "RS256",
                   :e "AQAB",
                   :kid "J/fnjLTd4bmKqN2owX5o+B5+xa7vD7CyR7SnhokO1Qc=",
                   :kty "RSA",
                   :n
                   "w4kOoaJQEYLrez9KH7H3BNNKeVKkeQ3KOk0xAG-2IVAPyVEZqF3iP6aJXgndm8mRmzpsSxMvJmYU_oHdDXBxQ5i-HVrfjYbTGWdeaYossPN_rSMCSJ_ukCsp6Kv0rweNUTL9YCrTi2TYaNlRqFylqP2QxvKNIV8gflb1G7tkaOdfayUmjE9tM9Pm7uacjoxJDnozQd0-JCuUHB7J2a-Na_bXuVNd1hxKKXiKXK77KMp5dx3DcStpaXjp0O3NK2Jov_nJjG5kNx0s5TjrqhhIGjfy10Ybe7P8220F-MIVC5TX4lIoA7Of2Qc0FCsVRyntAougNsV8cSfoGEZb_h3Wow",
                   :use "sig"}]})

(def region "eu-central-1")

(def url
  (str "https://cognito-idp."
       region
       ".amazonaws.com/"
       util-test/user-pool-id
       "/.well-known/jwks.json"))

(defn ctx
  [jwks-all]
  {:jwks-all (:keys jwks-all)
   :env {:region region}
   :auth {:user-pool-id util-test/user-pool-id
          :client-id util-test/user-pool-client-id
          :client-secret util-test/user-pool-client-secret}})

(deftest parse-token-test-expired-test
  (let [ctx (ctx jwks-key)]
    (with-redefs [util/get-current-time-ms (fn [] 1682668081782)]
      (is (= (assoc ctx
                    :body {:error {:aud :valid
                                   :exp :invalid
                                   :iss :valid
                                   :jwk :valid
                                   :signature :valid}})
             (jwt/parse-token ctx token))))))

(deftest parse-token-test
  (with-redefs [util/get-current-time-ms (fn [] 1602668081782)]
    (let [ctx (ctx jwks-key)]
      (is (= (merge ctx
                    {:user {:id "john.smith@example.com"
                            :email "john.smith@example.com"
                            :roles [:anonymous
                                    :group-3
                                    :group-2
                                    :group-1]}})
             (jwt/parse-token ctx token))))))

(deftest toke-expired-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
                  :body {:error {:jwk :invalid}})
           (jwt/parse-token ctx token-user-pool-2)))))

(deftest parse-invalid-signature-token-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
                  :body {:error {:jwk :valid
                                 :signature :invalid}})
           (jwt/parse-token ctx token-invalid-signature)))))

(deftest parse-valid-jwt-but-from-different-userpool-test
  (let [ctx (ctx jwks-key-2)]
    (with-redefs [util/get-current-time-ms (fn [] 1682668081782)]
      (is (= (assoc ctx
                    :body {:error {:aud :invalid
                                   :exp :invalid
                                   :iss :invalid
                                   :jwk :valid
                                   :signature :valid}})
             (jwt/parse-token ctx token-user-pool-2))))))

(deftest token-missing-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
                  :body {:error {:jwt :invalid}})
           (jwt/parse-token ctx nil)))))

(deftest token-not-valid-string-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
                  :body {:error {:jwt :invalid}})
           (jwt/parse-token ctx "invalit-token-here")))))

(def env {"Region" region})

(deftest test-fetching-jwks
  (with-redefs [util/get-env (fn [e]
                               (get env e))]
    (is (= {:something :here
            :jwks-all (:keys jwks-key)
            :env {:region region}}
           (jwt/fetch-jwks-keys {:something :here})))))

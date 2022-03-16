(ns lambda.jwt-test
  (:require [clojure.test :refer [deftest is]]
            [lambda.jwt :as jwt]
            [lambda.util :as util]
            [lambda.util-test :as util-test]))

(def token "eyJraWQiOiJTOWNDcTR5SVwvTW83cThhc241ZlFtaVFcL0k5YklrUTI5cmtrb1wvNTJmQmVnPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibnFRZ2ViM29USFFhYTEtMEtOY0stdyIsInN1YiI6IjdhMTE4MmY4LTFhN2UtNGY5YS05MGI0LTVmMmIwZWYyNGQzNyIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTMiLCJncm91cC0yIiwiZ3JvdXAtMSJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tXC9ldS1jZW50cmFsLTFfQUNYWXVsMDBRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGhAZXhhbXBsZS5jb20iLCJhdWQiOiI1ZmI1NTg0M2YxZG9qM2hqbHE3b25nZm1uMSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDcyOTc1LCJleHAiOjE2MzI0NzY1NzUsImlhdCI6MTYzMjQ3Mjk3NSwianRpIjoiYTg1NDUwZTMtYmMxOS00ODU5LTllOWMtMjY0NDZkMmFmYjMxIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.yFvBZZoSfAkAv6hCJ_XUiTJH8xi-SavQoNLWiw_BjKZkkjQdrapGem5GdIXi02oBbhNcLNKGbKMSn9SlQ4BvQxWalzeynhY703lH2d0yg1sAeSViGPC3Gh6TRstrLjyNYOL7A-qIM1m0HcT9ByyDUmTpgeXZx8itWMTHWb7KCQPTZc1Uza4rnHvwyg6Lnsu9kGIYYIqReyk2QuG9ggehdZ39AS3O152tfRMOgtLJytwwtjDj1kmDclKKQj8gnplq9bu3JORv1ull37M-Vm-gAg_XKbL3_acah1XoXOeekLhXC7jE6J5lNnZdKaAyN6uC6ojpOCFjAAEho8_yfGOsag")

(def token-invalid-signature "eyJraWQiOiJTOWNDcTR5SVwvTW83cThhc241ZlFtaVFcL0k5YklrUTI5cmtrb1wvNTJmQmVnPSIsImFsZyI6IlJTMjU2In0.eyJhdF9oYXNoIjoibnFRZ2ViM29USFFhYTEtMEtOY0stdyIsInN1YiI6IjdhMTE4MmY4LTFhN2UtNGY5YS05MGI0LTVmMmIwZWYyNGQzNyIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTMiLCJncm91cC0yIiwiZ3JvdXAtMSJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tXC9ldS1jZW50cmFsLTFfQUNYWXVsMDBRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGhAZXhhbXBsZS5jb20iLCJhdWQiOiI1ZmI1NTg0M2YxZG9qM2hqbHE3b25nZm1uMSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDcyOTc1LCJleHAiOjE2MzI0NzY1NzUsImlhdCI6MTYzMjQ3Mjk3NSwianRpIjoiYTg1NDUwZTMtYmMxOS00ODU5LTllOWMtMjY0NDZkMmFmYjMxIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.yFvBZZoSfAkAv6hCJ_XUiTJH8xi-SavQoNLWiw_BjKZkkjQdrapGem5GdIXi02oBbhNcLNKGbKMSn9SlQ4BvQxWalzeynhY703lH2d0yg1sAeSViGPC3Gh6TRstrLjyNYOL7A-qIM1m0HcT9ByyDUmTpgeXZx8itWMTHWb7KCQPTZc1Uza4rnHvwyg6Lnsu9kGIYYIqReyk2QuG9ggehdZ39AS3O152tfRMOgtLJytwwtjDj1kmDclKKQj8gnplq9bu3JORv1ull37M-Vm-gAg_XKbL3_acah1XoXOeekLhXC7jE6J5lNnZdKaAyN6uC6ojpOCFjAAEho8_yfGOssg")
(def token-user-pool-2
  "eyJraWQiOiJIUXFsd0pJaHJcL0srYUhGSWRUVlRxNzF5ZUo1MFNrbm9Mb0s0ZEdcL0huOXc9IiwiYWxnIjoiUlMyNTYifQ.eyJhdF9oYXNoIjoiQlpDNG5DdWxtOGZaVHlxcXRNSVhvZyIsInN1YiI6ImM4ZTE3YzI3LTYyNzctNGEyZi1iMDYxLTkxMTgxMzZmN2Q4MiIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAuZXUtY2VudHJhbC0xLmFtYXpvbmF3cy5jb21cL2V1LWNlbnRyYWwtMV9mM2J4Y0ltOFkiLCJjb2duaXRvOnVzZXJuYW1lIjoiam9obi5zbWl0aEBleGFtcGxlLmNvbSIsImF1ZCI6Ijdwa3FscmwyN2NnaDE4NGVpOGYydWt0ZzgiLCJldmVudF9pZCI6ImE2NWMwNWJhLWU0NWYtNGFiNi04YTJmLWUwOTFlNTAxNDRhYSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjMyNDc2MjQwLCJleHAiOjE2MzI0Nzk4NDAsImlhdCI6MTYzMjQ3NjI0MCwianRpIjoiNmJiYmJjN2ItODI4ZS00OWJkLTg2ZDMtNWQwYjI3NjI0MmMwIiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.YPcGoQbU40M1sWKNHSSaEb2Oa6OQKO66t8hrE8lpZG3IpT5h-hKIewFll0U8raO3t7qQP2OLwz3Q38REzMc9yfdLAwmLMEEKMrz3A0dLYO_vqoHInE9ptL97odeESNGtyLBJO0P5WhtZKT_RG-ss6tji0fP5z7G_0ly5E_ggYPb26c-5PfVbWffgIzg2kjNAcDG3zbD39vq5RP9XJfF-HnvPcLZxd0ZScQIaCDGv6kHDMZxIzPtMUo1bIwAgLr54hbANaSACYgwYYlorHO7kevt3X50NytztecczJF7AdBdsg6NIKx7rjgXjbXkM-PVG0pfB0mp-TDoIBZSvgjQMwg")

(def token-with-department
  "eyJraWQiOiJUZ2l3UXVHZFpCUlpXdHBCc1M4eDQ5Q1hjdVNEWnJ2dG9PeDg5M3NCMUxVPSIsImFsZyI6IlJTMjU2In0.eyJkZXBhcnRtZW50X2NvZGUiOiIwMDAiLCJhdF9oYXNoIjoiVWl2aF9HcXZMejh1ZDVrdmJRcE95QSIsInN1YiI6IjAwMGQwYzlkLWQ1ZjEtNGNiZC1hZGIyLWZhOTQ4MGJlMjM0NiIsImNvZ25pdG86Z3JvdXBzIjpbImxpbWUtcmlzay1tYW5hZ2VycyIsImxpbWUtYWNjb3VudC1tYW5hZ2VycyIsImxpbWUtbGltaXQtbWFuYWdlcnMiLCJyZWFsbS10ZXN0Il0sImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LWNlbnRyYWwtMS5hbWF6b25hd3MuY29tXC9ldS1jZW50cmFsLTFfUnNIRFY4YzhuIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGhAcmJpbnRlcm5hdGlvbmFsLmNvbSIsImdpdmVuX25hbWUiOiJKb2huIiwiYXVkIjoiMXVuajFrbG82ODU5bGFtOHZtYmt1aGszcjMiLCJldmVudF9pZCI6IjQzOTA5ZjAyLWFmNTQtNGVmMS05YjhmLTEyMmY3ZDk0YTQ5MCIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjQ2NjQ3NDAyLCJkZXBhcnRtZW50IjoiTm8gRGVwYXJ0bWVudCIsImV4cCI6MTY0NzUxMzA3MCwiaWF0IjoxNjQ3NDY5ODcwLCJmYW1pbHlfbmFtZSI6IlNtaXRoIiwiZW1haWwiOiJqb2huLnNtaXRoQHJiaW50ZXJuYXRpb25hbC5jb20ifQ.cXka3yiSMQo0pGtZ5MpR-POb5qnj8R6PnIXvspj2YzSAs7_qe4-dopMus4SPpyzmQzh0EXBh916-XMDdqQTeYQSmXkzRozNZfzc3lSc1w3v2F0iQH46wb4URw7Wzrg52beOz3Gcsk29Ag9Af-HZj_dmvCbuwHYRoHNm8TQvpGd9Gs1VB9pDPKTuVXda5iivpDX7CNSRfrFc85U0cvWacTR9Zd7rOTog-f5teDnEOfaSjlwPoD7dkobCZLXnRu99LWOEgy-GR1DoxfslnmaVQayBYe57MGcDYnqhAac0HDtxl1pjX1Phaqph5qqOo13wgkrphGSiBIDvDxwdnx3ckOg")

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

(def jwks-key-3 {:keys [{:alg "RS256"
                         :e "AQAB"
                         :kid "TgiwQuGdZBRZWtpBsS8x49CXcuSDZrvtoOx893sB1LU="
                         :kty "RSA"
                         :n "gUJYmaRYWlR5AcNH-pAwhTWArTBKTMW1DH2HbXE7sqCOy3XZt8T5X8TC-48I1Og05sdT-yoW2Amw8WmmHX8GB_U36RP3xA5wunQlJ-BLLJ77VA7Cy8AkDQZYmWwkfnNm0yKPOiADkcGQT0LEO_6BcGqV_h1Xc8FEsjhD4ovQIibr8JpQ86XNklItpquwzxP1nb48ZloI0gN4_K6tsH_E6cA_p8R0oohmeh1WIq2EWLFV1AOOg6qt17DLDfYDKthOtNceZTUYM25FkAqnsb-fB_Yz_nSsQt4-v6_k8WSA2fdevobz3j6u8yQ9R-712Q_xCvsuD4ZcKAzr4W6QEUdYjw"
                         :use "sig"}
                        {:alg "RS256"
                         :e "AQAB"
                         :kid "O5vEjcKWd4C94owJMyOKSOvAVX2pKjrevAg4rYbiaJc="
                         :kty "RSA"
                         :n "1v-98Xxku7kr-6EKviAkWiA8yKw8ZBSKB20LXWXX3W9VnC0lmBouTNqttRIiAFn0hEq4n9yj4yuhqwIH-oDwzL1O3ssR8liC5EzyLY_6ZNawg1377WWoO3A7-o72WnidZhCj-QM1VyBkxEa7yQyYf6pEpTagxlxaaonRw77r-vo8v6IqZZCf0HCkEXGh1B_1Umdn6agVAYQOva1GDtnAmGFqrBXDinDLqsuw3GJdSO6U-vJxpI6p4ftdDNy7lHEq3nAgvIQ8O7xHoqPAxpzVeoVAWNmrvQGVqzCr6ExFJnnH1gpBKkIUPPrgKjFL9YDXzdCB0HRjG4iq2ksPBbKWGQ"
                         :use "sig"}]})

(def region "eu-central-1")

(defn ctx [jwks-all]
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
    (let [ctx {:jwks-all (:keys jwks-key-3)
               :env {:region region}
               :auth {:user-pool-id "eu-central-1_RsHDV8c8n"
                      :client-id "1unj1klo6859lam8vmbkuhk3r3"
                      :client-secret util-test/user-pool-client-secret}}]
      (is (= (merge ctx
                    {:user {:id "john.smith@rbinternational.com"
                            :email "john.smith@rbinternational.com"
                            :roles [:anonymous
                                    :lime-risk-managers
                                    :lime-account-managers
                                    :lime-limit-managers
                                    :realm-test]
                            :department "No Department"
                            :department-code "000"}})
             (jwt/parse-token ctx token-with-department))))))

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

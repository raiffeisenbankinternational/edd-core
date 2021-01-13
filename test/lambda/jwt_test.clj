(ns lambda.jwt-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [lambda.jwt :as jwt]
            [lambda.util :as util]
            [lambda.util-test :as util-test]
            [lambda.test.fixture.client :as client]
            [lambda.util :as util])
  (:import (com.auth0.jwt.exceptions SignatureVerificationException)))

(def token "eyJraWQiOiI5WXpvZVVLdnlYc0cxaEVHbUIyT0h4XC9uZ0RBTXd2OEN5R0VNdlpMeHpzcz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJjMDgxODdlOS1mYTFiLTQ2YmEtOTE2My1iNmQ2MDRjZGJmMjAiLCJhdWQiOiI0OGxrczloMXJkM2t2MmYxMnYwb3V2ZzR1ZCIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTEiLCJncm91cC0yIiwiZ3JvdXAtMyJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXZlbnRfaWQiOiI2ZjE3OTExOS0yMmQ0LTQ0YTEtODczNC1kYTJmMWQyNmU4NTkiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTYwMjY2Nzg1MiwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LXdlc3QtMS5hbWF6b25hd3MuY29tXC9ldS13ZXN0LTFfQnRneWpwcDhRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGgiLCJleHAiOjE2MDI2NzE0NTIsImlhdCI6MTYwMjY2Nzg1MiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.JsuDwRqoMcto4ptIYiVurZ8xRA_sxHvJAolxzlsDOpTE8-mFldLdoo6L8BXChTdeWt609bdpjdn4iKPI9QMuOAQeRMEIW4LSfp3c1TSy5Hwcr_FuolExRSxBywRhW-Uxqq7uvStwlg0Db3J68UbT4FiZcQVY0TOg_olj9Hxj1IgEQ5-KkHHpHDNXy2cmwd8Drt5_prDD9fPT0fCOGV1alYX6fbrHu-r5HVsbfDarc4_GmD-lwza10LioQDK4eFxd0X2ho8SLnj6rKIi6YIV0zkq6Efs47DUwFwISiNkwmLYQ_k7DJ5B_UdoqRV5DNlJ0ErfHjhqSiYPeydqMEiwj2A")
(def token-invalid-signature "eyJraWQiOiI5WXpvZVVLdnlYc0cxaEVHbUIyT0h4XC9uZ0RBTXd2OEN5R0VNdlpMeHpzcz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJjMDgxODdlOS1mYTFiLTQ2YmEtOTE2My1iNmQ2MDRjZGJmMjAiLCJhdWQiOiI0OGxrczloMXJkM2t2MmYxMnYwb3V2ZzR1ZCIsImNvZ25pdG86Z3JvdXBzIjpbImdyb3VwLTEiLCJncm91cC0yIiwiZ3JvdXAtMyJdLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZXZlbnRfaWQiOiI2ZjE3OTExOS0yMmQ0LTQ0YTEtODczNC1kYTJmMWQyNmU4NTkiLCJ0b2tlbl91c2UiOiJpZCIsImF1dGhfdGltZSI6MTYwMjY2Nzg1MiwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLmV1LXdlc3QtMS5hbWF6b25hd3MuY29tXC9ldS13ZXN0LTFfQnRneWpwcDhRIiwiY29nbml0bzp1c2VybmFtZSI6ImpvaG4uc21pdGgiLCJleHAiOjE2MDI2NzE0NTIsImlhdCI6MTYwMjY2Nzg1MiwiZW1haWwiOiJqb2huLnNtaXRoQGV4YW1wbGUuY29tIn0.JsuDwRqoMcto4ptIYiVurZ8xRA_sxHvJAolxzlsDOpTE8-mFldLdoo6L8BXChTdeWt609bdpjdn4iKPI9QMuOAQeRMEIW4LSfp3c1TSy5Hwcr_FuolExRSxBywRhW-Uxqq7uvStwlg0Db3J68UbT4FiZcQVY0TOg_olj9Hxj1IgEQ5-KkHHpHDNXy2cmwd8Drt5_prDD9fPT0fCOGV1alYX6fbrHu-r5HVsbfDarc4_GmD-lwza10LioQDK4eFxd0X2ho8SLnj6rKIi6YIV0zkq6Efs47DUwFwISiMkwmLYQ_k7DJ5B_UdoqRV5DNlJ0ErfHjhqSiYPeydqMEiwj2A")
(def token-user-pool-2
  "eyJraWQiOiJxTG9IZWU5U21VZURYZExcL2tlR0dXemh2QitSY2ZWdkNrMmhHYkY3TlBsZz0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIwMTA0Mzc0ZS1kZDg4LTRlYWItYWI1NS1hOWJlMDQwZGZkZjgiLCJhdWQiOiI3djhwdGdmdmI1NjU2czduYzZsa3NtZDk1dSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJldmVudF9pZCI6IjQ5MjQ0NjlhLWQyNTAtNGM0ZS04MTBmLWM0Nzg2ODMwMGJiMSIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNjAyNjY3ODEwLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAuZXUtd2VzdC0xLmFtYXpvbmF3cy5jb21cL2V1LXdlc3QtMV9oNEFYbUdyYXYiLCJjb2duaXRvOnVzZXJuYW1lIjoiam9zaXAua292YWMiLCJleHAiOjE2MDI2NzE0MTAsImlhdCI6MTYwMjY2NzgxMCwiZW1haWwiOiJqb3NpcC5rb3ZhY0BleGFtcGxlLmNvbSJ9.FSd5gXKjn77U8iMuGGJ_4si8MUqClGBs2ZFG7LN3YsAG6SLjpH5PKUR-7TFxZ1WFfAQ3V4v9kK0O_M6YKfJcbghP3WMc1sYBPnn_OOOtaDd7hj-Z99kPudDfvaoZDvOd2XbORtPOotZT-rqX1nhKlk-vu8zSq9_y1b9TYHgnWJfNsvKGp5OEgW_gCZEZXfd2XNzkVR2W8KdvsffAFr9SFFaXP7sYiKeC62mk8Ijv1xW5aotEmaix-bbB7gEcUMr1ZXJGk16tzZXYg1EfWskrQ2EAnY1AQ3KeuT1A_7PQD1gHinGTyCZxy2oI0NFBzCAjueh7Rh5jP1VOAW_dL5PKgg")

(def jwks-key {:keys
               [{:alg "RS256",
                 :e   "AQAB",
                 :kid
                      "9YzoeUKvyXsG1hEGmB2OHx/ngDAMwv8CyGEMvZLxzss=",
                 :kty "RSA",
                 :n
                      "g4_75iTURga3GEPpsr5xV9w4npHrTyjrjFlRA_m3ZmM1u1mCJOZs2orQXrsEk55Bq6_rRNdTJ7VI_vjfDJiRoDRTXZh8YlyYYOlrztMHiK87MDqyjv3GfHMKefvT27yBFvWEM8u3OYOJzlXfHa55iSiS-YydWEeynJeBL41Vww_zQSl1XEqddHf3XC9c2lbJKbq7QWijmp3BjfUB_mXnneLVsmWoXwh7ChqMZYFP6MTxBJ9MwrOMciLIl-EeDZrvXeTFKuOwLk8a3qkpKW1xiThNx43NokTbBp2hCxSZ5W6Q7mST3Wm7lttL_ZSENOLtm7IXc6cDNR29gSSQRIg7Ww",
                 :use "sig"}
                {:alg "RS256",
                 :e   "AQAB",
                 :kid
                      "obk1friTTiaav3nd5pWwFRTeYqAEKIFayV6n5SocNeE=",
                 :kty "RSA",
                 :n
                      "jIFXCbFTH3N7oSAqBOoxHxVQ95nJrRMAttJ-hWEHK2vfwdo3BsDSnHJ3OHinsqkgOnd9saX5DdZG7nbx4UAMF56IIv5A3BTf7iK-Bxj3KUkhKpq6h9uXXufkwq1wx1o3uOGT-TKrzeuO0SBrxevl-g-rmuraLC0YWniCctLU8WXNm7kLJXGKSn86JCBeZgoM59qKYCRBmBGSpgHNA9pQLwPSvs0yEpG1Rw3xVJgHQV7ORwOLwCB6nPPGVlb6OE9BgsVKMTWvS7jijDiaGi82RHpJQrtYnGJ6YLdtQBW6bpV86MAfjiYzT1ftwmWXtEQst9u_D1g6rf_Mg2C56ygOmw",
                 :use "sig"}]})

(def jwks-key-2 {:keys
                 [{:alg "RS256",
                   :e   "AQAB",
                   :kid "qLoHee9SmUeDXdL/keGGWzhvB+RcfVvCk2hGbF7NPlg=",
                   :kty "RSA",
                   :n
                        "u0DTOuwyC3_52E6zHgP6DFgQtxl3ZoU5guTROMnSxu7nDodvdF8T3uwYJdDZ-q5DUCaw54MF4Ektqsx9QnZjsa_6JdmgY57Ll723FJTsP65CUQ_bKB-M7yd_1tC1FkysCxZwTFkiERpkKdv9WqkvzWGEBYcvcsPg4sV5N2PsFXTqdZs9INIWldnG_KSANyk-U9aRHd8zVtAw3Zwbu4LklaGfqG-7jpqjcCOM4_W5_WjGCHwaKlgLqvOtowuYmz2-jArPAHdVZMhJ_dFwiSCjiy_yR4ZrlnUHcZXpfxy3svEvRC3-lmjVA9InTc6h23bagfq65skZ7ERIGpCDxLffyQ",
                   :use "sig"}
                  {:alg "RS256",
                   :e   "AQAB",
                   :kid "8pgjFbBeR/7ARnYv8lryCiiKfYRPGAawTFocWdoZxQs=",
                   :kty "RSA",
                   :n
                        "hNjQ2vgkbV66d96FbzAVjkgswoQCzLEs5k1MqR93_WzBxzBNoQYQRbmm3Ypsr0Jx4Rjhi_JWj3_GH-APDxqsjZu-R50zkfCLoWe4R-KOdb-h09Y81mKqu3j9mqmdpe0nGCzuPgOGAbARt0y782spiRuclOM4yndXuqsFJzWw-T8UJ-Dn_85KfWCgjoiI85Y5s4vSVbgn64VPK5PWIGbQAYCCDbX7kVax4RQF7w8Ba-6DTm-VLK8gUgEOHgSQlZYsmlvjiNmcDbgxK0HGrdROO9lIUTrJm3JljZz8r1ObAHmdjJ4hhyCnMsqmF30XzTNjP3XG6aM5WI34X5cmFnni7Q",
                   :use "sig"}]}  )

(def region "eu-west-1")

(def url
  (str "https://cognito-idp."
       region
       ".amazonaws.com/"
       util-test/user-pool-id
       "/.well-known/jwks.json"))

(defn ctx
  [jwks-all]
  {:jwks-all (:keys jwks-all)
   :env      {:region region}
   :auth     {:user-pool-id  util-test/user-pool-id
              :client-id     util-test/user-pool-client-id
              :client-secret util-test/user-pool-client-secret}})

(deftest parse-token-test-expired-test
  (let [ctx (ctx jwks-key)]
    (with-redefs [util/get-current-time-ms (fn [] 1682668081782)]
      (is (= (assoc ctx
               :body {:error {:aud       :valid
                              :exp       :invalid
                              :iss       :valid
                              :jwk       :valid
                              :signature :valid}})
             (jwt/parse-token ctx token))))))

(deftest parse-token-test
  (with-redefs [util/get-current-time-ms (fn [] 1602668081782)]
    (let [ctx (ctx jwks-key)]
      (is (= (merge ctx
                    {:user {:id    "john.smith@example.com"
                            :email "john.smith@example.com"
                            :roles [:anonymous
                                    :group-1
                                    :group-2
                                    :group-3]}})
             (jwt/parse-token ctx token))))))

(deftest toke-expired-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
             :body {:error {:jwk :invalid}})
           (jwt/parse-token ctx token-user-pool-2)))))

(deftest parse-invalid-signature-token-test
  (let [ctx (ctx jwks-key)]
    (is (= (assoc ctx
             :body {:error {:jwk       :valid
                            :signature :invalid}})
           (jwt/parse-token ctx token-invalid-signature)))))

(deftest parse-valid-jwt-but-from-different-userpool-test
  (let [ctx (ctx jwks-key-2)]
    (with-redefs [util/get-current-time-ms (fn [] 1682668081782)]
      (is (= (assoc ctx
              :body {:error {:aud       :invalid
                             :exp       :invalid
                             :iss       :invalid
                             :jwk       :valid
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
            :jwks-all  (:keys jwks-key)
            :env       {:region region}}
           (jwt/fetch-jwks-keys {:something :here})))))

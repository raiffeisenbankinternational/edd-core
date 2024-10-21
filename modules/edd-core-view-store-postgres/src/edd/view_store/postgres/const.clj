(ns edd.view-store.postgres.const
  "
  Constant values.
  ")

(def COL_ID :id)

(def COL_AGGREGATE :aggregate)

(def SIMPLE_SEARCH_LIMIT 600)

(def ADVANCED_SEARCH_LIMIT 50)

(def AGGREGATE_FIELDS
  [:aggregate])

(def TABLE :aggregates)

(def TEST_REALM :test)

(def TAB_OPTIONS_1 :mv-options-one-field)
(def TAB_OPTIONS_2 :mv-options-two-fields)

(def SVC_TEST :edd-core)
(def SVC_DIMENSION :glms-dimension-svc)
(def SVC_APPLICATION :glms-application-svc)
(def SVC_CURRENCY :glms-currency-svc)
(def SVC_TASK_MANAGER :glms-task-manager-svc)

(def SERVICES
  #{:glms-access-rights-svc
    :glms-actions-svc
    :glms-api
    :glms-application-requests-svc
    SVC_APPLICATION
    :glms-archive-svc
    :glms-collateral-catalogue-svc
    :glms-consolidation-svc
    :glms-content-svc
    :glms-cpa-proxy-svc
    SVC_CURRENCY
    :glms-dataimport-svc
    SVC_DIMENSION
    :glms-document-svc
    :glms-exposure-svc
    :glms-facility-svc
    :glms-group-limitations-svc
    :glms-limit-review-report
    :glms-mail-svc
    :glms-notification-svc
    :glms-nwu-api
    :glms-plc2-svc
    :glms-post-auth-trigger-svc
    :glms-product-set-svc
    :glms-remarks-svc
    :glms-router-svc
    SVC_TASK_MANAGER
    :glms-template-svc
    :glms-upload-svc
    :glms-user-import-svc
    :glms-user-management-svc})

(ns lambda.logging.state
  "Shared state for logging configuration and context.
  This namespace is dependency-free to avoid circular dependencies.")

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^:dynamic *d-time-depth*
  "Current depth level for nested d-time measurements.
  Used for indentation in logging output."
  0)

;; Future: Add MDC (Mapped Diagnostic Context) vars here
;; (def ^:dynamic *mdc* {})

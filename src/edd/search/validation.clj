(ns edd.search.validation
  "Shared validation for view store operations.
   
   Provides Malli schemas and validation functions for consistent input validation
   across all view store implementations (Memory, S3, Postgres, Elastic).
   
   All implementations MUST use these validators to ensure compliance with
   the view store specification.")

;;; ============================================================================
;;; Malli Schemas
;;; ============================================================================

(def AggregateId
  "Schema for aggregate ID - must be a UUID"
  uuid?)

(def Version
  "Schema for aggregate version - must be a positive integer (> 0)"
  [:and int? [:fn {:error/message "Version must be positive (> 0)"} #(pos? %)]])

(def VersionOptional
  "Schema for optional aggregate version in queries"
  [:maybe Version])

(def Aggregate
  "Schema for aggregate in update-aggregate operations.
   Requires :id (UUID) and :version (positive int)."
  [:map
   [:id AggregateId]
   [:version Version]])

(def SnapshotQuery
  "Schema for get-snapshot query parameter.
   Can be either a UUID or a map with :id and optional :version."
  [:or
   AggregateId
   [:map
    [:id AggregateId]
    [:version {:optional true} VersionOptional]]])

(def Context
  "Schema for view store context.
   Must have [:meta :realm] set."
  [:map
   [:meta [:map
           [:realm [:or keyword? string?]]]]])

;;; ============================================================================
;;; Validation Functions
;;; ============================================================================

(defn validate-context!
  "Validates context for view store operations.
   Throws ex-info if context is invalid.
   
   Requirements:
   - Context must not be nil
   - Context must have [:meta :realm]"
  [ctx operation]
  (when-not ctx
    (throw (ex-info (str "Context cannot be nil for " operation) {})))
  (when-not (get-in ctx [:meta :realm])
    (throw (ex-info (str "Context must contain [:meta :realm] for " operation)
                    {:context (select-keys ctx [:meta :view-store :service-name])}))))

(defn validate-aggregate!
  "Validates aggregate for update-aggregate operations.
   Throws ex-info if aggregate is invalid.
   
   Requirements:
   - Aggregate must not be nil
   - Aggregate :id must be a UUID
   - Aggregate :version must be a positive integer (> 0)"
  [ctx aggregate]
  (let [{:keys [service-name]} ctx
        realm (get-in ctx [:meta :realm])]

    ;; Validate context first
    (validate-context! ctx "update-aggregate")

    ;; Aggregate must not be nil
    (when-not aggregate
      (throw (ex-info "Aggregate cannot be nil"
                      {:service-name service-name
                       :realm realm})))

    (let [{:keys [id version]} aggregate]
      ;; ID must exist
      (when-not id
        (throw (ex-info "Aggregate :id cannot be nil"
                        {:aggregate aggregate
                         :service-name service-name})))

      ;; ID must be UUID
      (when-not (uuid? id)
        (throw (ex-info "Aggregate :id must be UUID"
                        {:id id
                         :id-type (type id)
                         :service-name service-name})))

      ;; Version is required
      (when-not version
        (throw (ex-info "Aggregate :version is required"
                        {:aggregate aggregate
                         :service-name service-name})))

      ;; Version must be integer
      (when-not (integer? version)
        (throw (ex-info "Aggregate :version must be integer"
                        {:version version
                         :version-type (type version)
                         :service-name service-name})))

      ;; Version must be positive (> 0)
      (when-not (pos? version)
        (throw (ex-info "Aggregate :version must be positive (> 0)"
                        {:version version
                         :service-name service-name}))))))

(defn validate-snapshot-query!
  "Validates get-snapshot query parameters.
   Throws ex-info if parameters are invalid.
   
   Requirements:
   - Context must be valid (has realm)
   - ID must be a UUID
   - Version (if provided) must be a positive integer (> 0)
   
   Returns normalized {:id uuid :version int-or-nil}"
  [ctx id-or-query]
  ;; Validate context
  (validate-context! ctx "get-snapshot")

  ;; Normalize query
  (let [{:keys [id version]} (if (map? id-or-query)
                               id-or-query
                               {:id id-or-query})]

    ;; Validate ID
    (when-not (uuid? id)
      (throw (ex-info "ID must be UUID"
                      {:id id
                       :id-type (type id)})))

    ;; Validate version (if provided)
    (when (some? version)
      (when-not (integer? version)
        (throw (ex-info "Version must be integer"
                        {:version version
                         :version-type (type version)})))
      (when-not (pos? version)
        (throw (ex-info "Version must be positive (> 0)"
                        {:version version}))))

    ;; Return normalized query
    {:id id :version version}))

(defn validate-id-and-version!
  "Validates get-by-id-and-version parameters.
   Throws ex-info if parameters are invalid.
   
   Requirements:
   - ID must be a UUID
   - Version must be a positive integer (> 0)"
  [ctx id version]
  (validate-context! ctx "get-by-id-and-version")

  (when-not (uuid? id)
    (throw (ex-info "ID must be UUID"
                    {:id id
                     :id-type (type id)})))

  (when-not (integer? version)
    (throw (ex-info "Version must be integer"
                    {:version version
                     :version-type (type version)})))

  (when-not (pos? version)
    (throw (ex-info "Version must be positive (> 0)"
                    {:version version}))))

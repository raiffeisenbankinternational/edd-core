# Migration Guide

- `mock/query` no longer wraps response in `{:result ...}`
- Service name is now a keyword
- S3 `presign-url` changes:
  - New 2-arity `(presign-url ctx object)` accepts ctx with `:aws` key and an object map:
    ```clojure
    (s3/presign-url ctx {:method "PUT"
                         :expires 360
                         :object {:s3 {:bucket {:name "my-bucket"}
                                       :object {:key "path/to/file"}}}
                         :md5 "..."           ;; optional
                         :sha256 "..."        ;; optional
                         :content-length 123}) ;; optional
    ```
  - Additional headers (`content-md5`, `x-amz-content-sha256`, `content-length`) can now be signed via the object map or the `:headers` option in the lower-level arities
  - `ring.util.codec/form-encode` replaced with `sdk.aws.common/aws-form-encode` (ring dependency removed)
  - Host resolution now uses `get-host`, supporting custom S3 endpoints
  - Leading slashes in object paths are stripped automatically to avoid double-slash URLs
- `peek-state`, `verify-state`, and other accessor functions from `edd.test.fixture.dal` now strip `:meta` from results by default. Set `:keep-meta true` in mock state to preserve it.
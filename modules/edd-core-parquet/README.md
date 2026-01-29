# edd-core-parquet

Parquet file generation module for EDD-Core. Provides in-memory Parquet writing for columnar data export, optimized for analytics and data interchange.

## Installation

Add to your `deps.edn`:

```clojure
com.rbinternational.glms/edd-core-parquet {:mvn/version "VERSION"}
```

## Usage

```clojure
(require '[edd.parquet.core :as parquet])

;; Generate parquet bytes
(def parquet-bytes
  (parquet/write-parquet-bytes
    {:table-name "orders"
     :schema {:description "Customer orders"
              :columns [["ORDER_ID" :uuid "Order identifier" :required]
                        ["CUSTOMER" :string "Customer name" :required]
                        ["AMOUNT" :double "Order amount" :required]
                        ["STATUS" :enum "Order status" :required :enum ["PENDING" "SHIPPED" "DELIVERED"]]
                        ["CREATED" :date "Creation date" :optional]]}
     :rows [{"ORDER_ID" "123e4567-e89b-12d3-a456-426614174000"
             "CUSTOMER" "Acme Corp"
             "AMOUNT" 1234.56
             "STATUS" "PENDING"
             "CREATED" "2024-01-15"}]
     :compression :gzip
     :schema-version "1.0"}))

;; Upload to S3
(s3/put-object ctx {:bucket "my-bucket"
                    :key "exports/orders.parquet"
                    :body parquet-bytes
                    :content-type (parquet/mime-type)})
```

## API

### `write-parquet-bytes`

Writes rows to Parquet format in memory and returns `byte[]`.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `:table-name` | string | yes | Table name stored in Parquet metadata |
| `:schema` | map | yes | Schema definition (see below) |
| `:rows` | vector | yes | Sequence of row maps with string keys |
| `:compression` | keyword | no | `:gzip` (default) or `:uncompressed` |
| `:schema-version` | string | no | Version string stored in metadata |

### `mime-type`

Returns the MIME type for Parquet files: `"application/vnd.apache.parquet"`

### `get-codec`

Returns the Parquet `CompressionCodecName` for a keyword (`:gzip`, `:uncompressed`).

### `schema-fingerprint`

Returns a deterministic SHA-256 fingerprint of the schema definition. Useful for cache invalidation when schema changes.

## Schema Format

```clojure
{:description "Table description"
 :columns [["COLUMN_NAME" :type "Column description" :required/:optional & opts]
           ...]}
```

### Column Types

| Type | Parquet Type | Description |
|------|--------------|-------------|
| `:string` | BINARY (STRING) | UTF-8 string |
| `:enum` | BINARY (STRING) | String with optional validation |
| `:boolean` | BINARY (STRING) | Stored as `"TRUE"` / `"FALSE"` |
| `:uuid` | BINARY (STRING) | UUID as string |
| `:date` | INT32 (DATE) | Days since Unix epoch (1970-01-01) |
| `:double` | DOUBLE | 64-bit floating point |
| `:long` | INT64 | 64-bit signed integer |

### Enum Validation

For `:enum` columns, specify allowed values:

```clojure
["STATUS" :enum "Order status" :required :enum ["PENDING" "SHIPPED" "DELIVERED"]]
```

Invalid values throw `ExceptionInfo` at write time.

## Parquet Metadata

Generated files include key-value metadata:

| Key | Value |
|-----|-------|
| `table.name` | Table name |
| `table.description` | Schema description |
| `schema.fingerprint` | Deterministic schema hash |
| `schema.version` | Version string (if provided) |
| `column.{NAME}.description` | Column description |
| `column.{NAME}.requirement` | `"required"` or `"optional"` |
| `column.{NAME}.enum` | Allowed values (for enum columns) |

## GraalVM Native Image

This module includes reflection configuration for GraalVM native image compilation at:

```
resources/META-INF/native-image/com/rbinternational/glms/edd-core-parquet/reflect-config.json
```

## Dependencies

- `org.apache.parquet/parquet-hadoop` 1.16.0
- `org.apache.hadoop/hadoop-common` 3.4.2 (with CVE-related exclusions)
- `org.clojure/tools.logging` 1.2.4

## Development

```bash
# Run tests
make test

# Or directly
clj -M:test:unit
```

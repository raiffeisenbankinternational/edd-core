# TOFIX

## DynamoDB: TransactWriteItems exceeds 100-item limit

**Symptom**: Command producing many events (e.g. 114x `:tournament->category-added`) fails with
`java.util.zip.ZipException: Not in GZIP format` in `lambda.gzip/ungzip-response`.

**Root cause**: `store-results :dynamodb` (`src/edd/dynamodb/event_store.clj`) writes all
events + effects + identities + response-log as a single `TransactWriteItems` call.
DynamoDB limits `TransactItems` to 100 items (and 4 MB total). 114 events + 1 response-log
= 115 Puts -> HTTP 400 `ValidationException`.

**Masking bug**: `lambda.util/request` always sends `Accept-Encoding: gzip` and gunzips
when the response has `Content-Encoding: gzip`. DynamoDB gzip handling is unreliable for
small/error responses (AWS SDKs disable gzip for DynamoDB by default), so the uncompressed
400 error body fails gzip decoding and the real `ValidationException` is never surfaced.

**Required fixes**:

1. `store-transaction` must handle >100 items:
   - chunk into batches of <=100 Puts
   - atomicity trade-off: full request is no longer one transaction; put the
     response-log item in the LAST chunk so a partially written request is not
     marked successful
   - alternatively guard upstream and reject command batches producing >99 events
2. Stop masking DynamoDB errors:
   - do not send `Accept-Encoding: gzip` on DynamoDB requests (match AWS SDK defaults), or
   - make `ungzip-response` peek gzip magic bytes (`0x1f 0x8b`) via `PushbackInputStream`
     and pass body through unchanged when not actually gzipped

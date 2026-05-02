# edd-java-lambda-runtime

AWS Lambda runtime entrypoint for edd-core services. The `start` macro generates
a `lambda.Handler` class implementing `RequestStreamHandler` and
`org.crac.Resource`, so a single uberjar runs on the managed `java*` runtime with
**SnapStart** (CRaC) support.

```clojure
(edd.java-lambda-runtime.core/start
  (-> {} (event-store/register) (view-store/register) (my/register))
  edd/handler
  :filters [from-api from-queue from-bucket]
  :post-filter to-api)
```

## SnapStart / CRaC lifecycle

SnapStart snapshots the **initialized** execution environment once (at deploy /
version-publish) and restores that image on each cold start, skipping init.

- **`beforeCheckpoint`** (build time, captured in the snapshot — *not* on the
  restore path): runs a warm-up command through in-memory stores to load/JIT the
  request path into the snapshot, drops cached credentials so the snapshot never
  carries stale ones, then `System/gc` to shrink the snapshot.
- **`afterRestore`** (restore-critical — kept minimal): refreshes credentials
  (an env-var read in Lambda) and flags metrics to restart. On failure it clears
  the cache so the first invocation refreshes lazily.

Both hooks log their own elapsed time (`-beforeCheckpoint done in X ms`,
`-afterRestore done in X ms`) so restore cost is visible in CloudWatch.

### Why work is where it is

Local baseline (one cold JVM, restore-relevant operations):

| operation | time | phase | on restore path? |
|---|---|---|---|
| load (`require`) the runtime ns | ~6700 ms | init | no — snapshotted |
| warm-up (cold) | ~340 ms | `beforeCheckpoint` | no — snapshotted |
| `System/gc` | ~130 ms | `beforeCheckpoint` | no — build time |
| `aws.ctx/init` (credential refresh) | ~2 ms | `afterRestore` | yes |

The expensive ~6.7 s of namespace loading is paid once and captured in the
snapshot; only the ~2 ms credential refresh is on the restore path. That is the
point of SnapStart and why the warm-up belongs in `beforeCheckpoint`.

## Restore performance findings (measured on AWS)

Measured against a deployed SnapStart Lambda (`java21`), 30 cold-start samples per
configuration, each on a **freshly published snapshot** (same code/jar, only
memory changed). Numbers are AWS-reported `Restore Duration` in milliseconds.

| memory | n | mean | stdev | min | p50 | p95 | max |
|---|---|---|---|---|---|---|---|
| 1 GB   | 30 | 937.5  | 94.2  | 795.7 | 912.3  | 1105.7 | 1121.4 |
| 1.7 GB | 30 | 1114.1 | 72.2  | 982.6 | 1093.6 | 1257.8 | 1290.0 |
| 3 GB   | 30 | 1042.8 | 137.2 | 839.2 | 1033.7 | 1346.7 | 1347.8 |
| 4 GB   | 30 | 1140.6 | 117.8 | 973.8 | 1136.6 | 1334.3 | 1431.2 |

**Memory does not reduce cold restore.** Across 1–4 GB, restore stays ~0.9–1.1 s;
the differences are within noise (1 GB even had the lowest mean). Cold-tier
restore is **I/O-bound** (AWS fetches/loads the snapshot image), so extra vCPU
does not help.

**Cold tier vs warm tier.** A snapshot AWS has recently restored is cached on the
host (warm tier) and restores much faster (~200–350 ms), and *there* it is
CPU-bound so memory helps. But that caching is sustained-traffic-dependent and
cannot be relied on for post-deploy or low-traffic cold starts. Comparing a
warm-tier snapshot against a cold-tier one is the easiest way to draw a false
conclusion (e.g. "more memory made restore 3.6× faster" — it did not; the faster
run was simply warm-tier).

### Takeaways

- Do not raise memory to speed up restore; size it for the workload's CPU needs.
- The only code-side lever is a **smaller snapshot** (fewer loaded classes / less
  retained heap); the `beforeCheckpoint` GC already helps and the warm-up is a
  deliberate trade (bigger snapshot for a much faster first invocation — removing
  it was measurably worse).
- For guaranteed sub-second cold start under bursty/low traffic, use
  **Provisioned Concurrency** (no restore at all), at cost.

## Reproducing the measurement

Requires a deployed SnapStart Lambda exposed via API Gateway (the `e2e/` harness
in this repo deploys exactly this: `ping-svc` with `SnapStart: ApplyOn:
PublishedVersions`).

Pitfalls that invalidate results if ignored:

1. **Warm environments are reused** — a second burst against the same version
   produces almost no restores. Measure the **first** burst on a **fresh**
   published version (zero warm envs).
2. **Config-only changes must republish the version.** The Lambda `Version`
   `Description` includes the memory size (`code ${S3Key} mem ${LambdaMemorySize}`)
   precisely so a memory change creates a new snapshot; otherwise the `BUILD`
   alias keeps serving the old one.
3. **Cold tier vs warm tier** (above) — only compare fresh-snapshot first bursts.
4. **Extract from the `RESTORE_REPORT` line**, not `REPORT`: the `REPORT` line
   contains `Restore Duration` twice (the value, and inside `Billed Restore
   Duration`), which double-counts.

```bash
# Per memory M: publish a fresh snapshot, then fire N concurrent cold starts.
aws cloudformation deploy --stack-name e2etest-e2e-ping \
  --template-file e2e/cloudformation/lambda-svc.yaml --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides ... LambdaMemorySize="$M"     # new mem => new Version => fresh snapshot

API=$(aws cloudformation describe-stacks --stack-name e2etest-e2e-api \
        --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)
NOW=$(( $(date +%s) * 1000 ))
seq 1 30 | xargs -P 30 -I {} curl -s -o /dev/null -X POST "$API/ping-svc/query" \
  -H 'Content-Type: application/json' \
  -d '{"request-id":"#'"$(uuidgen)"'","interaction-id":"#'"$(uuidgen)"'","meta":{"realm":":test"},"query":{"query-id":":get-by-id","id":"#'"$(uuidgen)"'"}}'

# one value per cold restore:
aws logs filter-log-events --log-group-name /aws/lambda/e2etest-e2e-ping-svc \
  --start-time "$NOW" --filter-pattern '"RESTORE_REPORT"' \
  --query 'events[].message' --output text \
  | grep -oP "RESTORE_REPORT Restore Duration: \K[0-9.]+"
```

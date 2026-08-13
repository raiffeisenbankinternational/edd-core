# edd-core-security

Role-based authorization for edd-core commands and queries, built on the
`edd.core/reg-feature` mechanism. Policies are static data validated at
registration, compiled once at startup into an O(1) lookup index, and
evaluated before every command and top-level query handler.

## Registration

```clojure
(ns myapp.core
  (:require [edd.security :as security]))

(defn -main [& _args]
  (lambda/start
    (-> {}
        (assoc :service-name :myapp-svc)
        (module/register)
        (security/register
          :policies [...]
          :deps {:system-config (fn [_ _] {:query-id :get-system-config})}
          :mode :enforce))))
```

Options:

- `:policies` — vector of policy maps.
- `:deps` — deps resolved for every command and top-level query, available
  to conditions in the env. `:aggregate` is a reserved key.
- `:mode` — `:enforce` (default) or `:audit`. Audit evaluates everything and
  logs would-be denials but enforces nothing. Use it to roll out
  default-deny on a live service without a flag day.

There are NO built-in policies: everything not explicitly allowed is
denied, **including `:non-interactive` traffic** (effects re-entering the
service, service-to-service calls). A service with side effects must allow
it explicitly or workflows will die mid-chain:

```clojure
{:description "allow service-to-service"
 :principal {:role :non-interactive}
 :effect :allow
 :action :*}
```

## Policy format

Validated fail-fast at registration against a closed schema:

```clojure
{:description "am can update created orders"      ;; optional, echoed in errors/logs
 :principal   {:role :account-manager}            ;; only :role, :* = any role
 :effect      :allow                              ;; :allow | :deny
 :action      :update-order                       ;; also :update-*, :*, or [:a :b]
 :condition   [:map                               ;; optional malli schema
               [:aggregate [:map [:status [:= :created]]]]]}
```

`:action` matches command ids and query ids alike (exact keyword, `prefix-*`
wildcard, `:*`, or a vector of these). Wildcards are expanded against the
actually registered commands/queries once, at startup.

## Inline `:auth` on registrations

Instead of central policies, a command or query can carry its authorization
inline. The two modes are exclusive: either `security/register` gets
`:policies` and no registration carries `:auth`, or `:policies` is omitted
and each registration declares its own `:auth`. Mixing them fails init with
`"Central :policies and inline :auth are exclusive"`, naming the offending
registrations. In inline mode `:non-interactive` needs the same explicit
treatment: include it in the `:auth` roles of every action that effects or
other services invoke.

```clojure
(edd/reg-cmd ctx :approve-order handler
  :auth {:role [:supervisor :admin]              ;; keyword or vector = any of
         :fn (fn [ctx cmd]                       ;; optional, AND with :role
               (<= (get-in cmd [:attrs :amount])
                   (get-in ctx [:system-config :max-amount])))})

(edd/reg-query ctx :get-order get-order
  :auth {:role :account-manager})
```

- `:fn` receives the request ctx (with resolved deps, aggregate via
  `edd.el.ctx/get-aggregate`) and the command/query, returns truthy to allow.
- At init each `:auth` compiles into ordinary allow policies for that action,
  named `"auth <action-id>"` — evaluation, audit mode and error reporting
  work exactly as in central mode.
- `:auth` values are validated at startup (declared to the core via
  `reg-feature`'s `:schema`); a malformed `:auth` fails init.
- **`:auth` without `security/register` is inert** — nothing reads it, and
  without the module nothing is enforced at all. Registering the security
  feature is what turns enforcement on.

## Conditions

Any Malli schema, including `[:fn ...]`. The condition is validated against a
flat env map:

```clojure
(merge {:aggregate current-aggregate} resolved-security-deps)
```

- `:aggregate` is the current aggregate state; `nil` when the command creates
  a new aggregate, and for queries. Creation policies should assert on deps
  only.
- Cross-field checks are written as functions:

```clojure
{:description "owner may cancel while not shipped"
 :principal {:role :account-manager}
 :effect :allow
 :action :cancel-order
 :condition [:and
             [:map [:aggregate [:map [:status [:enum :created :paid]]]]]
             [:fn (fn [{:keys [aggregate current-user]}]
                    (= (:owner aggregate) (:id current-user)))]]}
```

## Evaluation

Per request, O(1): lookup by action id and the user's selected role
(`[:meta :user :role]`), then evaluate groups in order:

1. `:deny` policies — any condition hit denies
2. `:allow` policies — any hit allows
3. no hit at all — implicit deny

Explicit deny is absolute: an allow cannot punch a hole through a matching
deny; narrow the deny instead. Order within a group has no semantic effect,
it only picks which policy is named in logs. A condition that throws counts
as not matching, so a broken condition fails closed. Policies whose `:action`
matches no registered command or query are logged as warnings at startup.

**Deny is the exceptional escape hatch** (maintenance freeze, hard bans).
Express business rules as conditional allows over the implicit default-deny.
Remember deny conditions fire when the schema MATCHES — do not write them
allow-shaped.

A denial returns (commands) or throws (queries):

```clojure
{:error {:message "Forbidden"
         :cmd-id :cancel-order         ;; :query-id for queries
         :role :auditor
         :policy "creation is frozen"}} ;; :description of deciding policy,
                                        ;; or :implicit
```

Nothing is persisted and no effects are produced.

## Dependency resolution is not authorized

Authorization applies at the request boundary. Queries executed to resolve
`:deps` (command deps, query deps, security's own deps) bypass the filters —
otherwise every dependency lookup would need policies for every caller role.
Remote deps are authorized by the target service (arriving there as
`:non-interactive`).

## Testing

`with-mock-dal` initializes features exactly like the lambda runtime — no
extra setup:

```clojure
(mock/with-mock-dal
  (let [ctx (security/register (module/register mock/ctx)
                               :policies [...])]
    (mock/handle-cmd (assoc ctx :meta {:realm :test
                                       :user {:role :account-manager}})
                     {:commands [...]})))
```

`edd.security/evaluate` is a plain function if you want to assert policy
behavior directly without executing commands.

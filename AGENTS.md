# EDD-Core CQRS & Event Sourcing - LLM Implementation Guide

EDD-Core is a Clojure library for building event-sourced CQRS APIs. This guide provides everything needed to implement services using this architecture.

## Core Architecture

**Event Sourcing**: All state changes are stored as immutable events. Current state is derived by replaying events.

**CQRS**: Commands (write) are separated from Queries (read), allowing independent scaling and optimization.

**Request Flow**:
```
Command → Handler → Events → Event Store
                  ↓
                  Side Effects (reg-event-fx) → New Commands
                  ↓
                  Aggregate Update (reg-event)
```

## Essential Components

### 1. Context (`ctx`)
The context map flows through all handlers containing:
- `:service-name` - Current service identifier
- `:request-id` - UUID for request tracking
- `:interaction-id` - Session/correlation ID
- `:meta` - Metadata (realm, user, breadcrumbs)
- `:user` - User authentication info
- Injected dependencies from `:deps`

### 2. Command Registration (`reg-cmd`)

**Purpose**: Handle write operations that generate events.

**Signature**:
```clojure
(edd/reg-cmd ctx :command-id handler-fn
  :consumes schema        ;; Malli schema for validation
  :deps [:key dep-fn]     ;; Dependencies to inject
  :id-fn (fn [ctx cmd]))  ;; Optional: custom aggregate ID
```

**Handler Contract**:
```clojure
;; Input: ctx (with injected deps) and cmd
;; Output: event map or vector of events
(defn handler [ctx cmd]
  {:event-id :something-happened  ;; Required
   :id (:id cmd)                  ;; Aggregate ID
   :attrs {...}})                 ;; Event data
```

**Key Points**:
- Command handlers MUST be pure functions
- Return one event (map) or multiple events (vector)
- Use `:id-fn` for deterministic IDs (singletons, derived IDs)
- Commands are imperative: `:create-user`, `:update-order`, `:cancel-payment`

### 3. Event Handlers (`reg-event`)

**Purpose**: Apply events to aggregate state (materialized view).

**Signature**:
```clojure
(edd/reg-event ctx :event-id handler-fn)
```

**Handler Contract**:
```clojure
;; Input: current aggregate state and event
;; Output: new aggregate state
(defn handler [aggregate event]
  (assoc aggregate :field (:value event)))
```

**Key Points**:
- Event handlers build current state from event history
- MUST be pure, deterministic functions
- Events are past tense: `:user-created`, `:order-updated`, `:payment-cancelled`
- Common pattern: `(update agg :events conj event)` to store full history

### 4. Side Effects (`reg-event-fx`)

**Purpose**: Orchestrate workflows - trigger commands after events persist.

**Signature**:
```clojure
(edd/reg-event-fx ctx :event-id fx-handler)
```

**Handler Contract**:
```clojure
;; Input: ctx and event
;; Output: command map, or vector of command maps
(defn fx-handler [ctx event]
  {:service :target-service     ;; Target service (or current service)
   :commands [{:cmd-id :next-step
               :id (uuid/gen)
               :attrs {...}}]    ;; Vector of commands
   :meta {...}})                 ;; Optional metadata
```

**Return Formats**:
```clojure
;; Same service, single command
{:cmd-id :send-email :id (uuid/gen) :attrs {...}}

;; Same service, multiple commands  
[{:cmd-id :cmd-1 :id (uuid/gen)} 
 {:cmd-id :cmd-2 :id (uuid/gen)}]

;; Different service
{:service :other-svc
 :commands [{:cmd-id :remote-cmd :id (uuid/gen)}]}

;; Multiple services
[{:service :svc-1 :commands [...]}
 {:service :svc-2 :commands [...]}]
```

**Key Points**:
- Effects execute AFTER events are persisted (transactional)
- Use for: command chaining, fan-out, cross-service calls, notifications
- MUST be deterministic (no random UUIDs without seeding in tests)

### 5. Query Registration (`reg-query`)

**Purpose**: Read-only operations that don't modify state.

**Signature**:
```clojure
(edd/reg-query ctx :query-id handler-fn
  :consumes schema   ;; Input validation
  :produces schema   ;; Output validation
  :deps [:key dep])  ;; Optional dependencies
```

**Handler Contract**:
```clojure
(defn query-handler [ctx query]
  ;; Return any Clojure data structure
  {:result "data"})
```

### 6. Dependencies (`:deps`)

**Local Query Dependencies**:
```clojure
;; Define dependency function
(defn get-user-dep [_deps cmd]
  {:query-id :get-user
   :id (get-in cmd [:attrs :user-id])})

;; Register command with dependency
(edd/reg-cmd ctx :process-order handler
  :deps [:user get-user-dep])

;; Access in handler
(defn handler [ctx cmd]
  (let [{:keys [user]} ctx]  ;; Injected by framework
    (if (:active? user) ...)))
```

**Remote Service Dependencies**:
```clojure
(def get-inventory-dep
  {:service :inventory-svc
   :query (fn [_deps cmd]
            {:query-id :check-stock
             :product-id (get-in cmd [:attrs :product-id])})})

(edd/reg-cmd ctx :create-order handler
  :deps [:inventory get-inventory-dep])
```

**Parameterized Dependencies** (factories):
```clojure
(defn load-table [date-key]
  (fn [_deps cmd]
    {:query-id :load-table
     :date (get-in cmd [:attrs date-key])}))

(edd/reg-cmd ctx :compare-tables handler
  :deps [:current (load-table :current-date)
         :previous (load-table :previous-date)])
```

## Module Pattern (Recommended Structure)

**File**: `src/myapp/step/process_order.clj`
```clojure
(ns myapp.step.process-order
  (:require [edd.core :as edd]
            [myapp.schema :as s]))

;; 1. Define dependencies
(def get-inventory-dep
  {:service :inventory-svc
   :query (fn [_ cmd] 
            {:query-id :get-inventory
             :product-id (get-in cmd [:attrs :product-id])})})

;; 2. Command handler
(defn cmd [ctx {:keys [attrs] :as command}]
  (let [{:keys [inventory]} ctx]
    (if (>= (:quantity inventory) (:quantity attrs))
      {:event-id :order-created
       :id (:id command)
       :attrs attrs}
      {:event-id :order-rejected
       :id (:id command)
       :attrs {:reason :insufficient-inventory}})))

;; 3. Event handlers (build aggregate state)
(defn evt-created [agg event]
  (-> agg
      (assoc :status :created)
      (assoc :items (:items event))
      (update :events conj event)))

(defn evt-rejected [agg event]
  (-> agg
      (assoc :status :rejected)
      (assoc :reason (get-in event [:attrs :reason]))
      (update :events conj event)))

;; 4. Side effects (orchestration)
(defn fx-created [ctx event]
  ;; Trigger payment and inventory reservation
  [{:service :payment-svc
    :commands [{:cmd-id :charge-customer
                :id (uuid/gen)
                :attrs (select-keys (:attrs event) [:customer-id :amount])}]}
   {:service :inventory-svc
    :commands [{:cmd-id :reserve-items
                :id (uuid/gen)
                :attrs (select-keys (:attrs event) [:product-id :quantity])}]}])

(defn fx-rejected [ctx event]
  {:service :notification-svc
   :commands [{:cmd-id :notify-rejection
               :id (uuid/gen)
               :attrs (:attrs event)}]})

;; 5. Module registration (ALWAYS export this)
(defn register [ctx]
  (-> ctx
      (edd/reg-cmd :process-order cmd
                   :deps [:inventory get-inventory-dep]
                   :consumes s/ProcessOrderCmd)
      (edd/reg-event :order-created evt-created)
      (edd/reg-event :order-rejected evt-rejected)
      (edd/reg-event-fx :order-created fx-created)
      (edd/reg-event-fx :order-rejected fx-rejected)))
```

**Composing Modules** (`src/myapp/module.clj`):
```clojure
(ns myapp.module
  (:require [myapp.step.process-order :as process-order]
            [myapp.step.payment :as payment]
            [myapp.step.notification :as notification]))

(def register
  (comp process-order/register
        payment/register
        notification/register))
```

**Lambda Handler** (`src/myapp/core.clj`):
```clojure
(ns myapp.core
  (:require [edd.core :as edd]
            [lambda.core :as lambda]
            [myapp.module :as module]))

(defn -main [& _args]
  (lambda/start
    (-> {}
        (assoc :service-name :myapp-svc)
        (module/register))))
```

## Schema Definition (Malli)

```clojure
(ns myapp.schema
  (:require [malli.core :as m]))

(def ProcessOrderCmd
  [:map
   [:cmd-id [:= :process-order]]
   [:id uuid?]
   [:attrs
    [:map
     [:customer-id uuid?]
     [:product-id uuid?]
     [:quantity pos-int?]
     [:unit-price pos?]]]])

(def GetOrderQuery
  [:map
   [:query-id [:= :get-order]]
   [:id uuid?]])
```

## Testing with Mock DAL

**Basic Test Structure**:
```clojure
(ns myapp.step.process-order-test
  (:require [clojure.test :refer :all]
            [edd.test.fixture.dal :as mock]
            [myapp.step.process-order :as sut]
            [lambda.uuid :as uuid]))

(deftest test-successful-order
  (mock/with-mock-dal
    {:deps [{:service :inventory-svc
             :query {:query-id :get-inventory
                     :product-id #uuid "111..."}
             :resp {:quantity 100}}]}
    
    (let [cmd-id (uuid/gen)
          result (mock/handle-cmd
                   (sut/register mock/ctx)
                   {:commands [{:cmd-id :process-order
                                :id cmd-id
                                :attrs {:customer-id #uuid "222..."
                                        :product-id #uuid "111..."
                                        :quantity 5
                                        :unit-price 10.0}}]})]
      
      ;; Verify response
      (is (= {:events 1
              :effects 2  ;; payment + inventory
              :success true
              :sequences 0
              :identities 0
              :meta [{:process-order {:id cmd-id}}]}
             result))
      
      ;; Verify events in event store
      (mock/verify-state
        :event-store
        [{:event-id :order-created
          :id cmd-id
          :attrs {:customer-id #uuid "222..."
                  :product-id #uuid "111..."
                  :quantity 5
                  :unit-price 10.0}}])
      
      ;; Verify effects (commands sent to other services)
      (mock/verify-state
        :command-store
        [{:service :payment-svc
          :commands [{:cmd-id :charge-customer
                      :attrs {:customer-id #uuid "222..."
                              :amount 50.0}}]}
         {:service :inventory-svc
          :commands [{:cmd-id :reserve-items
                      :attrs {:product-id #uuid "111..."
                              :quantity 5}}]}]))))

(deftest test-insufficient-inventory
  (mock/with-mock-dal
    {:deps [{:service :inventory-svc
             :query {:query-id :get-inventory}
             :resp {:quantity 2}}]}  ;; Not enough
    
    (let [result (mock/handle-cmd
                   (sut/register mock/ctx)
                   {:commands [{:cmd-id :process-order
                                :id (uuid/gen)
                                :attrs {:quantity 5 ...}}]})]
      
      (mock/verify-state
        :event-store
        [{:event-id :order-rejected
          :attrs {:reason :insufficient-inventory}}]))))
```

**Testing Helpers**:
- `mock/ctx` - Base context with mock DAL
- `mock/with-mock-dal` - Sets up in-memory stores
- `mock/handle-cmd` - Execute command and return summary
- `mock/verify-state` - Assert store contents
- `:event-store`, `:command-store`, `:aggregate-store`, `:identity-store`

**Mock Structure**:
```clojure
(mock/with-mock-dal
  {:event-store [{:event-id :existing :id #uuid "..."}]  ;; Pre-existing events
   :aggregate-store [{:id #uuid "..." :status :active}]  ;; Pre-existing aggregates
   :deps [{:service :remote-svc                           ;; Mock remote queries
           :query {...}
           :resp {...}}]
   :responses [{:post "http://..." :body "{...}"}]}      ;; Mock HTTP
  
  ;; Test body
  ...)
```

## Request/Response Format

**Command Request**:
```clojure
{:request-id #uuid "..."        ;; Client-generated (idempotency)
 :interaction-id #uuid "..."    ;; Session ID
 :commands [{:cmd-id :create-order
             :id #uuid "..."    ;; Aggregate ID
             :attrs {...}}]}    ;; Command payload
```

**Query Request**:
```clojure
{:request-id #uuid "..."
 :interaction-id #uuid "..."
 :query {:query-id :get-order
         :id #uuid "..."}}
```

**Success Response**:
```clojure
{:result {:events 1
          :effects 2
          :success true
          :meta [{:create-order {:id #uuid "..."}}]}
 :invocation-id "..."
 :request-id #uuid "..."
 :interaction-id #uuid "..."}
```

**Error Response**:
```clojure
{:error {:message "Validation failed"
         :details {...}}
 :invocation-id "..."
 :request-id #uuid "..."
 :interaction-id #uuid "..."}
```

## Key Patterns

### Pattern 1: Command Validation
```clojure
(defn cmd [ctx {:keys [attrs] :as command}]
  (cond
    (invalid? attrs)
    {:event-id :validation-failed
     :error "Invalid input"}
    
    (business-rule-violated? ctx attrs)
    {:event-id :business-rule-failed
     :reason "..."}
    
    :else
    {:event-id :success
     :attrs attrs}))
```

### Pattern 2: Fan-Out to Multiple Aggregates
```clojure
(defn fx [ctx event]
  (for [item (get-in event [:attrs :items])]
    {:cmd-id :process-item
     :id (uuid/gen)  ;; Each item gets unique aggregate
     :attrs item}))
```

### Pattern 3: Conditional Side Effects
```clojure
(defn fx [ctx event]
  (when (> (get-in event [:attrs :amount]) 1000)
    {:service :fraud-detection-svc
     :commands [{:cmd-id :flag-for-review
                 :id (uuid/gen)
                 :attrs (select-keys (:attrs event) [:customer-id :amount])}]}))
```

### Pattern 4: Aggregate Query (get-by-id)
```clojure
(ns myapp.query
  (:require [edd.common :as common]))

(defn get-order [ctx {:keys [id]}]
  (common/get-by-id (assoc ctx :id id)))

(defn register [ctx]
  (edd/reg-query ctx :get-order get-order
                 :produces [:map [:id uuid?] [:status keyword?]]))
```

### Pattern 5: Singleton Aggregate (Single Instance per Service)
```clojure
(def singleton-id #uuid "00000000-0000-0000-0000-000000000001")

(edd/reg-cmd ctx :update-config handler
  :id-fn (constantly singleton-id))
```

## Critical Rules

1. **Determinism**: Command and event handlers MUST be pure functions
2. **Event Immutability**: Never modify events after creation
3. **ID Management**: Use `:id-fn` for derived IDs, client provides IDs otherwise
4. **Error Handling**: Return error events, don't throw exceptions in handlers
5. **Dependencies**: All external data MUST be declared in `:deps`
6. **Side Effects**: Only `reg-event-fx` triggers new commands
7. **Testing**: Always use `mock/with-mock-dal` for tests
8. **Schemas**: Define Malli schemas for all commands and queries
9. **Module Pattern**: One `register` function per namespace
10. **Event Naming**: Past tense (`:created`, `:updated`, `:failed`)

## Database Tables (Automatic)

- **event_store**: Immutable event log (source of truth)
- **command_request_log**: All incoming commands
- **command_response_log**: Responses (for deduplication)
- **command_store**: Effect commands (for tracing)
- **aggregate_store**: Materialized views (ElasticSearch/Postgres)

**Optimistic Locking**: `event_seq` prevents concurrent updates to same aggregate

## Common Errors

1. **"No handler found"**: Forgot to register command/query
2. **"Invalid command registration"**: Missing required params (`:consumes`)
3. **"oldString not found"**: Schema validation failed
4. **Infinite loops**: Check breadcrumbs depth (max 20)
5. **UUID conflicts**: Use `(uuid/gen)` for new IDs, not hardcoded values

## Quick Reference

```clojure
;; Command: Write operation → generates events
(edd/reg-cmd ctx :cmd-id handler
  :consumes schema
  :deps [:key dep-fn]
  :id-fn (fn [ctx cmd]))

;; Event: Apply to aggregate state
(edd/reg-event ctx :event-id 
  (fn [agg event] new-agg))

;; Effect: Trigger new commands after event
(edd/reg-event-fx ctx :event-id
  (fn [ctx event] commands))

;; Query: Read-only operation
(edd/reg-query ctx :query-id handler
  :consumes schema
  :produces schema)

;; Testing
(mock/with-mock-dal
  {:deps [{:service :svc :query {...} :resp {...}}]}
  (mock/handle-cmd ctx {:commands [...]})
  (mock/verify-state :event-store [...]))
```

This guide provides everything needed to build production-grade event-sourced services with EDD-Core.

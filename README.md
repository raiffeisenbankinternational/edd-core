# Event Driven Design Elements (edd-el)

Simple lib to support building Event-Sourcing applications. 


## Philosophy
<blockquote>
  It is not a framework!
</blockquote>

The main idea behind this library was not to be a framework,
but rather usefull set of function that will express our target
architecture design. 

It is designed with freedom. Library expresses desire to have 
stat pushed to the edge of the system and to help engineers
develop mathematically pure functions. But it has no limitations
on what you can do (i.e. You can use directly DB connection at 
any time and make stuff dirty). 

## Bootstraping

# Architecture

Entire system is build around 2 main components:
1. Event store
2. Aggregate store

# Supported event store implementation
Currently, implementations supports following event store implementation: 
* PostgreSQL
* DynamoDB

# Supported aggregate store implementation
Currently, only one aggregate store is supported:
* ElasticSearch

## Commands
To register commands user `reg-cmd`

```
==>(:require 
       [edd.]))
```

## Register effects
Output of each individual command are events. Based on events we can determine 
if there are any other actions needed to be executed it he system. This can be
triggering another service or calling same service recursively (i.e Send email 
after user was created). Commands that are created based on events are called
`effects`. Effects are stores together with events in a database transactionaly
and are executed as commands on target services asynchronously. 

Effects are stored transactionally to make sure that we are not triggering any
action that is not valid (i.e. Send email when creating user if user creation 
has failed or rolled-back). 

Effect handlers are registered declarative using `edd-core/reg-event-fx`. 

Example of effect registration: 

``` clojure

; When user-create event is creted, trigger command :send email to the same service
(edd.core/reg-event-fx :user-created
    (fn [ctx events]
         [{:id "2"
           :cmd-id :send-email}]))

; When user-create event is creted, trigger command :send email to the :email-sending-svc
(edd.core/reg-event-fx :user-created
    (fn [ctx events]
         [{:service :email-sending-svc
           :commands [{:id "2"
                       :cmd-id :send-email}]
                       :meta {}}]))
```

Output of fx handler can be either vector or map. If output is map it can contain `:service` 
keyword which would indicate that target is another service. In that case we have to have
actual commands inside `:commands` vector

## JSON Serialization and De-Serialization

Serialization is implemented using `metosin/jsonista` (Crrently using 
fork of metosin `alpha-prosoft/jsonista`). Implementation is actually 
using jackson. Special metion here is required because there is some 
customization done for serializing keyword values and uuid data type. 

All keyward **values** are prefixed with ":". If value is already 
containing ":" it will duplicate first ":". All uuid values will be
serialized prefixed with "#". 

```clojure
=> (:require  [lambda.util :as util])
=> (util/to-json {:a :b})
"{\"a\":\":b\"}"
=> (util/to-json {:a #uuid "d13d8c5c-2704-4fe1-8938-c339db9db15c"})
"{\"a\":\"#d13d8c5c-2704-4fe1-8938-c339db9db15c\"}"
=> (util/to-json {:a "#some"})
"{\"a\":\"##some\"}"
```
 
```clojure
=> (:require  [lambda.util :as util])
=> (util/to-edn "{\"a\":\":b\"}")
{:a :b}
=> (util/to-edn "{\"a\":\"#d13d8c5c-2704-4fe1-8938-c339db9db15c\"}")
{:a #uuid "d13d8c5c-2704-4fe1-8938-c339db9db15c"}
=> (util/to-edn "{\"a\":\"##some\"}")
{:a "#some"}
```

## Building modules

All handlers are easily composable. Handler registration is just currently just adding 
keys to map. Modules are not feature of library. It is just way how to structure code. 

Following is example of project structure with module: 

``` clojue 

```

## Mocking external dependecies 

When testing code against edd test fixtures you can declare how dependencies to external
services will be resolved. Example: 

``` clojure
(mock/with-mock-dal
      {:deps [{:service        :remote-svc
               :request-id     request-id
               :interaction-id interaction-id
               :meta           meta
               :query          {:param "Some Value"}
               :resp           {:remote :response}}]
       :event-store []
       ...})
      ....
      )
```

## UUID Gen
In namespce `lambda/uuid` there is helper function `gen` which can be used
got generating uuid's. 

UUID generation is tricky from testing perspective. To help with that there
is implementation of with-state mock is provided. Example of usage:

```clojure 
=> (:require [clojure.test :refer :all]
             [lambda.test.fixture.state :as state]
             [lambda.uuid :as uuid])
=> (deftest test-input
    (let [uuid1 (uuid/gen)
          uuid2 (uuid/gen)  
          uuid3 (uuid/gen)]
      (state/with-state
        (with-redefs [uuid/gen (fn []
                             (state/pop-item
                              :uuid/gen
                              [uuid3 uuid2 uuid1]))]
          (is (= uuid1
              (uuid/gen)))
          (is (= uuid2
              (uuid/gen)))
          (is (= uuid3
              (uuid/gen)))
          (is (= nil
              (uuid/gen)))))))

```

## Authentication
I hearby declare authentication quite unflexible. When using 
from-api filter then user each request is being checked for 
X-Authorization header (X-Authorization is used instead of 
standard because AWS LB and API Gateway sometimes take it over).
Header should contain signed JWT token. 

You can expect to get structure like:
```
(def ctx 
  {:user {:id "user-id"
          :email "user@email"
          :role :user-role}}
```
User role will be slected based on roles present in JWT token. Currently
only cognito is supported. If there are more than one role possible, it is
expected to receive `{:selected-role :user-role}` as part of request body. 
If this is not the case then user wil receive anonymous role:
```
(def ctx 
  {:user {:id "anonymous"
          :email "anonymous"
          :role :anonymous}}
```




# How to test

## Mock Data access layer

Most of the mocking could be done with clojure **with-redefs** macro.
For more convenience the namespace **edd.test.fixture.dal** contains couple useful macros
mocking Data Access Layer. The mocking is done by redefining Data Access Layer function and binding
state to thread local. So any business logic relays on the database could be tested without using
in memory database like H2 or test containers.

### Macro: with-mock-dal

This macro redefines all Data Access Layer functions and binds a state which currently is an atom.
**Caution:** If any Data Access Layer function is added this macro is needed to be extended.

### Macro: verify-state

This macro tests if our expectations about database state are correct, if not test will fail.

### Example

In this test with-mock-dal macro is called with body where event is stored, state assertion
is done, checking if in memory database contains correct event.

```
(deftest when-store-and-load-events-then-ok
  (with-mock-dal (dal/store-event {} {} {:id 1 :info "info"})
    (verify-state [{:id 1 :info "info"}] :event-store)
    (let [events (dal/get-events {} 1)]
      (is (= [{:id 1 :info "info"}]
             events)))))
```

In this test with-mock-dal macro is called with body where aggregate is updated multiple times
and Data Access Layer function search is called upon this aggregate.

```
(deftest when-query-aggregate-with-unknown-condition-then-return-nothing
  (with-mock-dal (dal/update-aggregate {} {:id 1 :payload "payload"})
    (dal/update-aggregate {} {:id 2 :payload "payload"})
    (dal/update-aggregate {} {:id 3 :payload "pa2"})
    (is (= [{:id 3 :payload "pa2"}]
           (dal/simple-search {} {:id 3})))
    (is (= []
           (dal/simple-search {} {:id 4})))))
```

# Database Semantics

## Table Semantics
### command_request_log
This table will contain all command requests, including those incoming from the client (e.g. FE), and also those resulting from effect commands.
For a documentation of the breadcrumbs column please see the section about breadcrumbs below.
Events created for a command will have the same [request_id,breadcrumbs] combination in the _event_store_ table as the corresponding _command_request_log_ entry, except events created by incoming commands will have a breadcrumbs value of "0" in the events table, instead of the blank value in _command_request_log_.
CAVEAT: Single requests can result in multiple entries in this table if there are retries for a request, e.g. if a browser will repeat a request after network issues. Also AWS does not guarantee that a lambda will be triggered exactly once per request. In the future there will be a _retry_ column in this table to state if a given request resulted from a retry to allow filtering for this.
command_request_log is written before transactions start. If a transaction needs to be rolled back, then all the other tables will not be written. To find out which requests in _command_request_log_ were not processed successfully they can be identified by request_ids for which there are no corresponding entries in _command_response_log_.
Fields to be added:
- retry_count: integer; increments with each retry
- error_message: if a request failed, error message will be put there
- success: boolean; true if request was processed successfully

### command_response_log
Contains the response sent to the client (e.g. FE).
Only if a command was processed successfully, an entry will be written here containing the response sent by the backend to the client (FE), compare documentation about _command_request_log_.
There is a field _success_ contained in the _data_ column json document, but it will always be true, because requests that fail will not be written to _command_response_log_.
The _command_response_log_ is used for de-duplication. If a request is identified with a [request_id,breadcrumbs] combination that already exists in this table, then the request will not be processed again but the response will be taken from _command_response_log_. Technically, the request's transaction will fail because of the unique [request_id,breadcrumbs] constraint is violated. The system can recover from this situation by checking the _command_response_log_ whether the request was already processed successfully, and if so return the success message to the client.

### command_store
Contains only the commands created in course of effects, not the original commands as issued by the client (e.g. FE).
It also lists the _source_service_ and _target_service_ for a command.
Can be used to identify effect communication edges between service nodes. Breadcrumbs will be in sync with breadcrumbs in the _command_request_log_.
Breadcrumbs can be used to trace back from _commands_store_ to the original requests in _command_request_log_.

### event_store
Whenever an event handler was processed successfully, an entry will be written to _event_store_, also containing the contents of the event itself.
There is a unique constraint on the tuple [service_name,aggregate_id,event_seq] to support optimistic locking, so concurrent requests will not accidentally modify one and the same entity.

## Scoped IDs
### invocation_id
_invocation_id_ is an identifier from AWS, which identifies the request that triggered the function invocation. It is copied to the _context_ map from request headers (field lambda-runtime-aws-request-id). It is mainly used to search the logs, to map command execution to log entries.
### request_id
The _request_id_ is generated by the client (e.g. FE), when a new request to the backend is done. It has to be unique on a per request base, and the client is responsible for keeping this contract.
CAVEAT: If the request-id is re-used, edd-core will return a cached response for the given request.
In the context of one request, multiple commands and corresponding events can be generated.
One and the same request-id is re-used for all effects triggered by a service call.

### interaction_id
Semantically this mostly is a session-id. On opening a user session in the client (e.g. FE browser), one interaction-id is created and used during the whole session. _interaction_id_s come in handy when debugging tests (to tell one test's outcomes from another's).

## Breadcrumbs
Generally, breadcrumbs trace inter-service-calls caused by effects. Using breadcrumbs it is possible to trace the sequence of downstream service calls triggered by effects.
Sample values:
- '' or '0' for original requests. _command_request_log_ will have blanks, the other tables will have '0's.
- '0:0' first effect command triggered on original svc
- '0:1' second effect command triggered on original svc
- '0:1:0' first effect command on transitive svc
- ...

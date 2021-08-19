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
on what you can dou (i.e. You can use directly DB connection at 
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
      {:dps [{:service        :remote-svc
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

# Changelog

## Changes

- [AP-800] Implement test suite for event store compliance

- [CARS-4941] Rework edd-core attributes
- update dimension attributes
- add tests for the new attrs

- [AP-798] Remove :sequence feature
This commit removes the :sequence concept from all edd-core event store implementations.
The sequence feature allowed event handlers to returna
{:sequence :keyword :id aggregate-id} maps to generate
gapless incrementing sequence numbers with bidirectional
lookup between aggregate IDs and sequence numbers.
The feature requires distributed locking and complex two-phase commits
in Postgres (using LOCK TABLE and dual-table updates).

This cannot be efficiently or reliably implemented in DynamoDB or other
eventually-consistent data stores without significant complexity and
potential for race conditions. It is also easy to replace with simple
aggregate that will used as counter (testable, and more flexible)

- [CARS-4852] Refactor edd-core attributes file (intenal)

- [CARS-4853] Update gitleaks configuration to TOML
Format with fingerprint-based allowlist. Secrets
all are anyway public and we didi history rewrite

- [CARS-4670] Add edd-client submodule
- add edd.client.core namespace to reach other services

- [CARS-4437] Fix empty context in edd core
- replace deps map with a full context
- update tests

- [AP-795] Fix build to work properly

- [CARS-4540] Update gitleaks for test secrets

- [CARS-4404] Increase loop depth support

- [CARS-4340] Add some functions to the edd core io module
- add minor io functions
- add tests
- update changes file

- [LSO-16349] Fix typo in identity query

- [CARS-4101] Add better logging to have durations
This way we can see what is bottleneck.

- [CARS-3906] Allow endpoint override for all operations

- [LSP-10890] Load lambda.logging explicitly
We should ensure it is AOT compiled if needed on runtime

- [CARS-3085] Disable metrics locally with an env var
- add env var condition in lambda core
- update readme

- [CARS-3081] Handle exceptions properly
To tel lambnda function that message was not successfully
processes we need to throw exception because it will
not bre re-tried.

- [LSP-10872] Fix malli parser in edd-postgres module
- bump malli to the latest version
- mitigate new parsing response (remove tags)

- [LSO-16510] Implement logging for local de
Add simple hierarchical loogger. Each d-time will be logged in its on
ident

- [LSO-16507] Add a comment to the parser module
- add a comment to the parser module about malli
- update makefile

- [LSP-10813] Improve edd-core (errors, logs, metrics)
FEATURE:
- disable metrics then an env var set
- detailed log in command handler

FIX:
- for java runtime we need new *request* atom for each request

- [LSP-10688] Reduce dependencies in json module
- exclude external libraries from the net.mikera/vectorz library
- additional make targets
- run the changes script earlier in pre-build.sh

- [LSP-10680] Create edd-json module

- [LSO-15555] Remove deps to ring
We dont need ring and want to use URLEncoder from
JVM. But there are special requirements for AWS
url encoding:

https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html#sigv4-create-canonical-request-steps

- [LSO-15825] Update compojure to 1.7.1
- move compojure to edd-core-dev/deps.edn
- leave explicit requirement for ring-codec in main deps.edn

- [LSO-15805] Initialitize runtime only once

- [LSO-15727] Fix cache creation for CRAC Java Runtime

- [LSO-15556] Unify runtimes
We want to make sure that we can use CRAC (SnapStart) and keep all
existing functionality

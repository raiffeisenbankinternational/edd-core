# Changelog

## Changes

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

#!/bin/bash

set eux -pipefail

export DatabasePassword="no-secret"
export DatabaseEndpoint="127.0.0.1"

flyway -password="${DatabasePassword}" \
           -schemas=glms,test,prod,test_local_svc \
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -cleanDisabled="false" \
           clean 

flyway -password="${DatabasePassword}" \
           -schemas=test\
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -locations="filesystem:${PWD}/sql/files/edd" \
           migrate 

flyway -X -password="${DatabasePassword}" \
           -schemas=prod \
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
            -locations="filesystem:${PWD}/modules/edd-core-view-store-postgres/migrations" \
           migrate 

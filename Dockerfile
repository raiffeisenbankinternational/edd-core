ARG DOCKER_URL
ARG DOCKER_ORG
ARG ARTIFACT_ORG

FROM ${DOCKER_URL}/${DOCKER_ORG}/common-img:b966

# Custom build from here on
ENV PROJECT_NAME edd-core

ARG ARTIFACT_ORG
ENV ARTIFACT_ORG ${ARTIFACT_ORG}

COPY resources resources
COPY src src
COPY test test
COPY modules modules
COPY deps.edn deps.edn
COPY tests.edn tests.edn
COPY format.sh format.sh

RUN ./format.sh check

RUN set -e && clj -M:test:unit

COPY sql sql

ARG BUILD_ID

RUN set -e &&\
    echo "Org: ${ARTIFACT_ORG}" &&\
    export AWS_DEFAULT_REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region) &&\
    TARGET_ACCOUNT_ID="$(aws sts get-caller-identity | jq -r '.Account')" &&\
    cred=$(aws sts assume-role \
                --role-arn arn:aws:iam::${TARGET_ACCOUNT_ID}:role/PipelineRole \
                --role-session-name "${PROJECT_NAME}-deployment-${RANDOM}" \
                --endpoint https://sts.${AWS_DEFAULT_REGION}.amazonaws.com \
                --region ${AWS_DEFAULT_REGION}) &&\
    export AWS_ACCESS_KEY_ID=$(echo $cred | jq -r '.Credentials.AccessKeyId') &&\
    export AWS_SECRET_ACCESS_KEY=$(echo $cred | jq -r '.Credentials.SecretAccessKey') &&\
    export AWS_SESSION_TOKEN=$(echo $cred | jq -r '.Credentials.SessionToken') &&\
    export AccountId=$TARGET_ACCOUNT_ID &&\
    export EnvironmentNameLower=pipeline &&\
    export DatabasePassword="$(aws secretsmanager get-secret-value  \
                                       --secret-id /pipeline/alpha-postgres-svc/password \
                                       --query SecretString \
                                       --output text)" &&\
    export DatabaseEndpoint="$(aws rds describe-db-instances \
                                       --query 'DBInstances[0].Endpoint.Address' \
                                       --output text)" &&\
    domain_name=$(aws es list-domain-names  | jq -r '.DomainNames[0].DomainName') &&\
    echo "Found domain ${domain_name}" &&\
    domain_url=$(aws es describe-elasticsearch-domain --domain-name ${domain_name} | jq -r '.DomainStatus.Endpoints.vpc') &&\
    export IndexDomainEndpoint=$domain_url &&\
    export DatabaseEndpoint="$(aws rds describe-db-instances --query 'DBInstances[].Endpoint.Address' --filter "Name=engine,Values=postgres" --output text)" &&\
    flyway -password="${DatabasePassword}" \
               -schemas=glms,test,prod \
               -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
               clean &&\
    flyway -password="${DatabasePassword}" \
           -schemas=test\
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -locations="filesystem:${PWD}/sql/files/edd" migrate &&\
    flyway -password="${DatabasePassword}" \
            -schemas=prod \
            -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
            -locations="filesystem:${PWD}/sql/files/edd" migrate &&\
    clj -M:test:it &&\
    echo "Building b${BUILD_ID}" &&\
    clj -M:jar  \
       --app-group-id ${ARTIFACT_ORG} \
       --app-artifact-id ${PROJECT_NAME} \
       --app-version "1.${BUILD_ID}" &&\
    cp pom.xml /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar.pom.xml &&\
    ls -la target &&\
    cp target/${PROJECT_NAME}-1.${BUILD_ID}.jar /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar &&\
    mvn install:install-file \
         -Dfile=target/${PROJECT_NAME}-1.${BUILD_ID}.jar \
         -DgroupId=${ARTIFACT_ORG} \
         -DartifactId=${PROJECT_NAME} \
         -Dversion="1.${BUILD_ID}" \
         -Dpackaging=jar &&\
    cd modules &&\
    for i in $(ls); do \
       cd $i &&\
       clj -M:jar  \
             --app-group-id ${ARTIFACT_ORG} \
             --app-artifact-id ${i} \
             --app-version "1.${BUILD_ID}" &&\
       pom.xml /dist/release-libs/${i}-1.${BUILD_ID}.jar.pom.xml &&\
       target/${i}-1.${BUILD_ID}.jar /dist/release-libs/${i}-1.${BUILD_ID}.jar; \
       cd ..; \
    done &&\
    cd .. &&\
    rm -rf /home/build/.m2/repository &&\
    rm -rf target &&\
    tree /dist

RUN ls -la




RUN cat pom.xml

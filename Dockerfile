ARG DOCKER_URL
ARG DOCKER_ORG
ARG ARTIFACT_ORG

FROM ${DOCKER_URL}/${DOCKER_ORG}/common-img:latest

# Custom build from here on
ENV PROJECT_NAME edd-core

ARG ARTIFACT_ORG
ENV ARTIFACT_ORG ${ARTIFACT_ORG}

COPY --chown=build:build resources resources
COPY --chown=build:build src src
COPY --chown=build:build test test
COPY --chown=build:build modules modules
COPY --chown=build:build deps.edn deps.edn
COPY --chown=build:build tests.edn tests.edn
COPY --chown=build:build build.clj build.clj
COPY --chown=build:build format.sh format.sh
COPY --chown=build:build ansible ansible

RUN ./format.sh check

COPY --chown=build:build sql sql

USER root


RUN mkdir -p /home/jenkins
RUN chown build:build /home/build -R &&\
    chown build:build /home/jenkins -R &&\
    chown build:build /dist -R


USER build

ARG BUILD_ID
ENV BUILD_ID=${BUILD_ID}
ARG BUILD_NUMBER
ENV BUILD_NUMBER=${BUILD_NUMBER}

RUN set -e &&\
    echo "Org: ${ARTIFACT_ORG}" &&\
    PROJECT_NAME="api" clojure -M:test:unit &&\
    export TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" -s) &&\
    export AWS_DEFAULT_REGION=$(curl -H "X-aws-ec2-metadata-token: $TOKEN" -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region) &&\
    export AWS_REGION=$AWS_DEFAULT_REGION &&\
    export TARGET_ACCOUNT_ID="$(aws sts get-caller-identity | jq -r '.Account')" &&\
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
    export DatabasePassword="no-secret" &&\
    export DatabaseEndpoint="127.0.0.1" &&\
    echo "Purging all SQS" &&\
    for queue in $(aws sqs list-queues --query 'QueueUrls[]' --output text); do \
      aws sqs purge-queue --queue-url $queue; \
    done &&\
    export IndexDomainScheme=http &&\
    export IndexDomainEndpoint=127.0.0.1:9200 &&\
    flyway -password="${DatabasePassword}" \
           -schemas=glms,test,prod,test_local_svc \
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -cleanDisabled="false" \
           clean &&\
    flyway -password="${DatabasePassword}" \
           -schemas=test\
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -locations="filesystem:${PWD}/sql/files/edd" \
           migrate &&\
    flyway -password="${DatabasePassword}" \
           -schemas=prod \
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
            -locations="filesystem:${PWD}/modules/edd-core-view-store-postgres/migrations" \
           -X \
           migrate &&\
    echo "Building b${BUILD_ID}" &&\
    clojure -T:build jar+install \
       :app-group-id ${ARTIFACT_ORG} \
       :app-artifact-id ${PROJECT_NAME} \
       :app-version $(printf '"1.%s"' ${BUILD_ID})  &&\
    cp pom.xml /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar.pom.xml &&\
    ls -la target &&\
    cp target/${PROJECT_NAME}-1.${BUILD_ID}.jar /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar &&\
    echo "Building modules" &&\
    env &&\
    cd modules &&\
    for i in $(ls -1 | sort); do \
       cd $i &&\
       echo "Building module: ${i}" &&\
       cat deps.edn &&\
       clojure -Stree &&\
       clojure -M:test:it &&\
       clojure -M:test:unit &&\
       clojure -J-Dedd-core.override=1.${BUILD_ID} -T:build jar+install  \
          :app-group-id ${ARTIFACT_ORG} \
          :app-artifact-id ${i} \
          :app-version $(printf '"1.%s"' ${BUILD_ID}) &&\
       cp pom.xml /dist/release-libs/${i}-1.${BUILD_ID}.jar.pom.xml &&\
       cp target/${i}-1.${BUILD_ID}.jar /dist/release-libs/${i}-1.${BUILD_ID}.jar; \
       if [[ $? -gt 0 ]]; then exit 1; fi &&\
       cd ..; \
    done &&\
    cd .. &&\
    echo "Running integration tests: $(pwd)" &&\
    env &&\
    clojure -M:test:it &&\
    rm -rf /home/build/.m2/repository &&\
    rm -rf target &&\
    tree /dist

RUN ls -la




RUN cat pom.xml

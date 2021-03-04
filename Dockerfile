ARG DOCKER_URL
ARG DOCKER_ORG

FROM ${DOCKER_URL}/${DOCKER_ORG}/common-img:b767

ARG DOCKER_URL
ARG DOCKER_ORG

# Custom build from here on
ENV PROJECT_NAME edd-core

COPY resources resources
COPY src src
COPY test test
COPY deps.edn deps.edn
COPY tests.edn tests.edn

RUN set -e && clj -A:test:unit

RUN set -e &&\
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
           -schemas=glms \
           -url=jdbc:postgresql://${DatabaseEndpoint}:5432/postgres?user=postgres \
           -locations="filesystem:${PWD}/sql/files" migrate &&\
    clj -A:test:it

ARG BUILD_ID
RUN echo "Building b${BUILD_ID}" &&\
    set -e && clj -A:jar  \
  --app-group-id ${DOCKER_ORG} \
  --app-artifact-id ${PROJECT_NAME} \
  --app-version "1.${BUILD_ID}"

RUN ls -la

RUN ls -la target


RUN cp pom.xml /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar.pom.xml
RUN cp target/${PROJECT_NAME}-1.${BUILD_ID}.jar /dist/release-libs/${PROJECT_NAME}-1.${BUILD_ID}.jar

RUN cat pom.xml

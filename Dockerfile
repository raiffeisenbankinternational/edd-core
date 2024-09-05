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
COPY --chown=build:build features features
COPY --chown=build:build test test
COPY --chown=build:build modules modules
COPY --chown=build:build deps.edn deps.edn
COPY --chown=build:build tests.edn tests.edn
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

RUN set -e &&\
    echo "Org: ${ARTIFACT_ORG}" &&\
    clojure -M:test:unit &&\
    export AWS_DEFAULT_REGION=$(curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region) &&\
    export AWS_REGION=$AWS_DEFAULT_REGION &&\
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
    export IndexDomainScheme=https &&\
    export IndexDomainEndpoint=$domain_url &&\
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
    echo "Run ansible stuff" &&\
    ansible-playbook ansible/deploy/deploy.yaml &&\
    echo "Building b${BUILD_ID}" &&\
    clojure -M:jar  \
       --aot "clojure.java.io" \
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
         -DpomFile=pom.xml \
         -Dversion="1.${BUILD_ID}" \
         -Dpackaging=jar &&\
    echo "Building modules" &&\
    env &&\
    cd modules &&\
    for i in $(ls); do \
       cd $i &&\
       echo "Building module $i" &&\
       bb -i '(let [build-id "'${BUILD_ID}'" \
                    lib (symbol `edd-core/edd-core) \
                    new-lib (symbol "'${ARTIFACT_ORG}/${PROJECT_NAME}'") \
                    deps (read-string \
                          (slurp (io/file "deps.edn"))) \
                    global (get-in deps [:deps lib]) \
                    deps (if global \
                           (assoc-in deps [:deps new-lib] {:mvn/version (str "1." build-id)}) \
                           deps) \
                    aliases [:test] \
                    deps (reduce \
                           (fn [p alias] \
                             (if (get-in p [:aliases alias :extra-deps new-lib]) \
                               (assoc-in p [:aliases alias :extra-deps new-lib] \
                                         {:mvn/version (str "1." build-id)}) \
                               p)) \
                           deps \
                           aliases)] \
                (spit "deps.edn" (with-out-str \
                                   (clojure.pprint/pprint deps))))' &&\
       cat deps.edn &&\
       clojure -Stree &&\
       clojure -M:test:it &&\
       clojure -M:test:unit &&\
       clojure -M:jar  \
             --aot "clojure.java.io" \
             --app-group-id ${ARTIFACT_ORG} \
             --app-artifact-id ${i} \
             --app-version "1.${BUILD_ID}" &&\
       cp pom.xml /dist/release-libs/${i}-1.${BUILD_ID}.jar.pom.xml &&\
       cp target/${i}-1.${BUILD_ID}.jar /dist/release-libs/${i}-1.${BUILD_ID}.jar; \
       if [[ $? -gt 0 ]]; then exit 1; fi &&\
       cd ..; \
    done &&\
    cd .. &&\
    echo "Running integration tests: $(pwd)" &&\
    env &&\
    aws sqs purge-queue \
          --queue-url "https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AccountId}/${AccountId}-${EnvironmentNameLower}-it" &&\
    aws sqs purge-queue \
          --queue-url "https://sqs.${AWS_DEFAULT_REGION}.amazonaws.com/${AccountId}/${AccountId}-${EnvironmentNameLower}-it.fifo" &&\
    clojure -M:test:it &&\
    rm -rf /home/build/.m2/repository &&\
    rm -rf target &&\
    tree /dist

RUN ls -la




RUN cat pom.xml

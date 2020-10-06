ARG DOCKER_URL
ARG DOCKER_ORG
ARG BUILD_ID

FROM ${DOCKER_URL}/${DOCKER_ORG}/common-img:b73

ARG DOCKER_URL
ARG DOCKER_ORG
ARG BUILD_ID

# Custom build from here on
ENV PROJECT_NAME edd-core

COPY resources resources
COPY src src
COPY test test
COPY deps.edn deps.edn

RUN set -e && clj -A:test:runner

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

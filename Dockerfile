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
COPY --chown=build:build build.sh build.sh
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

# Run the build script with proper error handling
RUN chmod +x build.sh && ./build.sh

RUN ls -la




RUN cat pom.xml

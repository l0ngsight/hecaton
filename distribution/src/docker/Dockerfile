# Hecaton member image (plan §1.2): the slim distribution on a Temurin 25 JRE.
#
#   docker build --build-arg HECATON_VERSION=<version> -t hecaton/hecaton:<version> \
#       -f distribution/src/docker/Dockerfile distribution/target
#
# Configure with HECATON_CONFIG (a path inside the image, default config/hecaton-docker.xml),
# HC_* environment variables and JAVA_OPTS, as with bin/hc start.
FROM eclipse-temurin:25-jre

ARG HECATON_VERSION
ENV HECATON_HOME=/opt/hecaton \
    HECATON_CONFIG=config/hecaton-docker.xml \
    PATH=/opt/hecaton/bin:$PATH

# Security updates of the base image's packages, and no Pebble: the base image's service manager
# (/usr/bin/pebble), which the member does not use and whose Go runtime has known CVEs
RUN apt-get update \
    && apt-get -y upgrade \
    && rm -rf /var/lib/apt/lists/* /usr/bin/pebble \
    && groupadd --system hecaton \
    && useradd --system --gid hecaton --home-dir /opt/hecaton --shell /usr/sbin/nologin hecaton

ADD hecaton-${HECATON_VERSION}-slim.tar.gz /opt/
RUN mv /opt/hecaton-${HECATON_VERSION}-slim /opt/hecaton \
    && chown -R hecaton:hecaton /opt/hecaton

WORKDIR /opt/hecaton
USER hecaton
EXPOSE 5701
LABEL org.opencontainers.image.title="Hecaton" \
      org.opencontainers.image.description="Hecaton in-memory data grid member" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.url="https://hecaton.org.uk/" \
      org.opencontainers.image.source="https://github.com/l0ngsight/hecaton" \
      org.opencontainers.image.version="${HECATON_VERSION}"

ENTRYPOINT ["hc", "start"]

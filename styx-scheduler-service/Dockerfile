FROM registry.spotify.net/spotify/trusty-java:0.22
MAINTAINER dataex <dataex-squad@spotify.net>

ENTRYPOINT ["/bin/bash", "-c", "exec /usr/bin/java $JVM_DEFAULT_ARGS $JVM_ARGS -jar /usr/share/styx-scheduler/styx-scheduler.jar \"$@\"", "bash"]

ENV GOOGLE_APPLICATION_CREDENTIALS /etc/spotify/gcloud/steel-ridge-91615.json

ADD target/lib                        /usr/share/styx-scheduler/lib
ADD target/styx-scheduler-service.jar /usr/share/styx-scheduler/styx-scheduler.jar

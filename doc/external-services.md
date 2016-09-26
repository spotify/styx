# External Services

Styx uses [Google Cloud Datastore] and [Google Cloud Bigtable] for storing data about workflow
state and executions and [Google Container Engine] \(GKE\) for container orchestration.

The connection to these external services uses Service Accounts and Application Default
Credentials ([ADC]) to authenticate.

## Container Engine, Kubernetes

Because of the typical containers that we run at Spotify and the fact that they need to resolve
resources in our own data centers, the k8s cluster needs some tweaking. These manual tweaks are
temporary workarounds for features that are currently lacking in GKE.

## Datastore

A Datastore namespace will be used to store application and scheduler state.

## Bigtable

The Bigtable cluster can be created from the cloud console and used as is. For the typical
workloads that Styx generate, a basic 3 node cluster is sufficient.

# Configuration keys

TBA

[Google Cloud Datastore]: https://cloud.google.com/datastore/
[Google Cloud Bigtable]: https://cloud.google.com/bigtable/
[Google Container Engine]: https://cloud.google.com/container-engine/
[ADC]: https://developers.google.com/identity/protocols/application-default-credentials

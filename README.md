# Styx

A data processing job scheduler for Kubernetes 

## Description

Styx is a service that is used to trigger periodic invocations of docker containers. The
information needed to schedule such invocations, is read from a set of files on disk or an
external service providing such information. The service takes responsibility for triggering
and possibly also re-triggering invocations until a successful exit status has been emitted
or some other limit has been reached. Styx is built using the [Apollo] framework and uses
[Kubernetes] for container invocations.

Styx can optionally provide some dynamic arguments to container executions that indicates
which time period a particular invocation belongs to. For example an hourly job for the first
hour of 2016-01-01 might have the dynamic argument --datetime=2016-01-01T00 appended to the
container invocation. At the time of writing, styx has no concept of checking whether an execution
is needed for a particular time or not. It simply executes containers for the current time and
continues until a particular workflow is disabled.

The envisioned main use case for styx is to execute big data pipelines, possibly long running
processes that transform data periodically. It's initial use case is to run jobs written using
[luigi]

## Key concepts

The key type of information that styx concerns itself with is Workflows. A Workflow is either
enabled or disabled and is associated with a ScheduleDefinition. ScheduleDefinition objects hold
information used to figure out when a job needs to be executed and other things such as which
docker image to use for execution. When it is time to run a specific Workflow, styx creates a
WorkflowInstance that references a Workflow and also holds the concrete parameters that needs
to be provided for a container invocation. Styx also keeps track of WorkflowInstance executions
and provides information about them via the API.

### More docs

* [Styx design](doc/design-overview.md)
* [External services](doc/external-services.md)
* [API Specification](doc/api.apib)


## Usage

TBA


[Kubernetes]: http://kubernetes.io/
[Apollo]: https://spotify.github.io/apollo/
[luigi]: https://github.com/spotify/luigi

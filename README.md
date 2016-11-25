# Styx

[![CircleCI](https://circleci.com/gh/spotify/styx/tree/master.svg?style=shield)](https://circleci.com/gh/spotify/styx)
[![License](https://img.shields.io/github/license/spotify/styx.svg)](LICENSE)

A data processing job scheduler for Kubernetes 

## Description

Styx is a service that is used to trigger periodic invocations of docker containers. The
information needed to schedule such invocations, is read from a set of files on disk or an
external service providing such information. The service takes responsibility for triggering
and possibly also re-triggering invocations until a successful exit status has been emitted
or some other limit has been reached. Styx is built using the [Apollo] framework and uses
[Kubernetes] for container orchestration.

Styx can optionally provide some dynamic arguments to container executions that indicates
which time period a particular invocation belongs to. For example an hourly job for the first
hour of 2016-01-01 might have the dynamic argument `--datetime 2016-01-01T00` appended to the
container invocation.

The envisioned main use case for Styx is to execute data processing job, possibly long running
processes that transform data periodically. It's initial use case is to run jobs written using
[Luigi], but it does not have any hard ties to Luigi. Styx can just as well execute a container
with some simple bash scripts.

Styx was build to function smoothly on Google Cloud Platform, thus it makes use of Google products
such as Google Cloud Datastore, Google Cloud Bigtable and Google Container Engine. However, the 
integrations with these products are all done through clear interfaces and other backends can 
easily be added.

## Key concepts

The key type of information that Styx concerns itself with are Workflows. A Workflow is either
enabled or disabled and is has a Schedule. A Schedule specifies how often a Workflow should be 
triggered, which docker image to run and which arguments to pass to it on each execution. Each time
a Workflow is triggered, a Workflow Instance is created. The Workflow instance is tracked as 
'active' until at least on execution of the docker image returns with a 0 exit code. Styx will keep
track of Workflow Instance executions and provides information about them via the API.

### More docs

* [Styx design](doc/design-overview.md)
* [External services](doc/external-services.md)
* [API Specification](doc/api.apib)


## Usage

### Setup

A fully functional Service can be found in [styx-standalone-service](./styx-standalone-service). 
This packaging contains both the API and Scheduler service in one artifact. This is how you build
and run it.

Some configuration keys in
[`styx-standalone.conf`](./styx-standalone-service/src/main/resources/styx-standalone.conf) have
to be specified for the service to work:

Configure which Google services clusters/instances to use:

```yaml
# gke cluster
styx.gke.default.project-id = ""
styx.gke.default.cluster-zone = ""
styx.gke.default.cluster-id = ""

# bigtable instance
styx.bigtable.project-id = ""
styx.bigtable.instance-id = ""

# datastore config
styx.datastore.project-id = ""
styx.datastore.namespace = ""
```

To monitor a different local directory for schedule definitions, edit this line:

```yaml
# directory to monitor for schedule definitions
styx.source.local.dir = "/etc/styx"
```

Build the project:

```bash
> mvn package
```

Run the service:

```bash
> java -jar styx-standalone-service/target/styx-standalone-service.jar
```

### Workflow schedule configuration

To define a schedule, simply write a  yaml file to `/etc/styx` (given the above configuration)

`/etc/styx/my-schedules.yaml`
```yaml
schedules:
  - id: my-workflow
    partitioning: hours
    docker_image: my-workflow:0.1
    docker_args: ['./run.sh', '{}']
```

- `schedules` **[schedule]**: The main key, containing a list of schedules

- `schedule[].id` **string**: A unique identifier for the workflow (lower-case-hyphenated)
 - This identifier is used to refer to the workflow through the API.

- `schedule[].partitioning` **string**: How often the workflow should be triggered
 - Allowed values are `hourly`, `daily`, `weekly`
  - *todo: support more intervals*
  - *todo: support cron syntax*

- `schedule[].docker_image` **string**: The docker image that should be executed
 - The docker image that contains the workflow

- `schedule[].docker_args` **[string]**: The arguments passed to the docker image
 - This list should only contain strings. These will be passed as the arguments to the docker
container. Any occurrences of the `"{}"` placeholder argument will be replaced with the current
partition date or datehour. Note that it must be quoted in the yaml file in order no to be
interpreted as an object.

Example arguments for the supported partitioning values:
```
- hourly - 2016-04-01T14, 2016-04-01T15, ... (UTC hours)
- daily  - 2016-04-01,    2016-04-02,    ...
- weekly - 2016-04-04,    2016-04-11,    ... (Mondays)
```

### Triggering and executions

Each time a Workflow Schedule is triggered, Styx will treat that trigger as a first class
entity. Each Trigger will have at least one Execution which can potentially take a long time
to execute. If another Trigger happens during this time, both triggers will be active, each
with one running container. Because Styx treats each Trigger individually, it can ensure that
each one of them complete successfully.

Styx does not have any assumptions about what is executed in the container, it only cares about
the exit code. Any execution returning a non-zero exit code will cause a re-try to be scheduled,
with an exponential back-off between each try.

---

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are
expected to honor this code.

[Kubernetes]: http://kubernetes.io/
[Apollo]: https://spotify.github.io/apollo/
[Luigi]: https://github.com/spotify/luigi
[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md

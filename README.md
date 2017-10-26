# Styx

[![CircleCI](https://circleci.com/gh/spotify/styx/tree/master.svg?style=shield)](https://circleci.com/gh/spotify/styx)
[![Coverage Status](https://codecov.io/gh/spotify/styx/branch/master/graph/badge.svg)](https://codecov.io/gh/spotify/styx)
[![License](https://img.shields.io/github/license/spotify/styx.svg)](LICENSE)
[![Release](https://jitpack.io/v/spotify/styx.svg)](https://jitpack.io/#spotify/styx)

A batch job scheduler for Kubernetes

## Description

Styx is a service that is used to trigger periodic invocations of Docker containers. The information
needed to schedule such invocations, is read from a set of files on disk or an external service
providing such information. The service takes responsibility for triggering and possibly also
re-triggering invocations until a successful exit status has been emitted or some other limit has
been reached. Styx is built using the [Apollo] framework and uses [Kubernetes] for container
orchestration.

Styx can optionally provide some dynamic arguments to container executions that indicates which time
period a particular invocation belongs to. For example an hourly job for the first hour of
2016-01-01 might have the dynamic argument `2016-01-01T00` appended to the container invocation.

The envisioned main use case for Styx is to execute data processing job, possibly long running
processes that transform data periodically. Its initial use case is to run workflows of jobs
orchestrated using [Luigi], but it does not have any intrinsic ties to Luigi. Styx can just as well
execute a container with some simple bash scripts.

Styx was built to function smoothly on Google Cloud Platform, thus it makes use of Google products
such as Google Cloud Datastore, Google Cloud Bigtable and Google Container Engine. However, the
integrations with these products are all done through clear interfaces and other backends can easily
be added.

## Key concepts

The key concept that Styx concerns itself with is Workflows. A Workflow is either enabled or
disabled and has a Schedule. A Schedule specifies how often a Workflow should be triggered, which
Docker image to run and which arguments to pass to it on each execution. Each time a Workflow is
triggered, a Workflow Instance is created. The Workflow instance is tracked as 'active' until at
least one execution of the Docker image returns with a 0 exit code. Styx keeps track of Workflow
Instance executions and provides information about them via the API.

## Development status

Styx is actively being developed and deployed internally at Spotify where it is being used to run
around 2200 production workflows. Because of how we build and integrate infrastructure components at
Spotify, this repository does not contain a GUI at the time of writing, while we do have one
internally. The goal is to break out more of these components into open source projects that
complement each other.

### More docs

* [Styx design]
* [External services]
* [API Specification](doc/api.apib) - [HTML version](https://spotify.github.io/styx/api.html)

## Usage

### Setup

A fully functional Service can be found in [styx-standalone-service](./styx-standalone-service).
This packaging contains both the API and Scheduler service in one artifact. This is how you build
and run it.

The following configuration keys in
[`styx-standalone.conf`](./styx-standalone-service/src/main/resources/styx-standalone.conf) have
to be specified for the service to work:

```yaml
# Google Container Engine (GKE) cluster
styx.gke.default.project-id = ""
styx.gke.default.cluster-zone = ""
styx.gke.default.cluster-id = ""

# Google Cloud Bigtable instance
styx.bigtable.project-id = ""
styx.bigtable.instance-id = ""

# Google Cloud Datastore config
styx.datastore.project-id = ""
styx.datastore.namespace = ""
```

Build the project:

```bash
> mvn package
```

Run the service:

```bash
> java -jar styx-standalone-service/target/styx-standalone-service.jar
```

### Workflow configuration

Refer to [API Specification](https://spotify.github.io/styx/api.html#workflows-workflows-post) for how to deploy a workflow.

```
id: my-workflow
docker_image: my-workflow:0.1
docker_args: ['./run.sh', '{}']
schedule: hourly
offset: PT1H
secret:
  name: my-secret
  mount_path: /etc/my-keys
service_account: my-service-account@my-project.iam.gserviceaccount.com
```

#### `id` **[string]**
A unique identifier for the workflow (lower-case-hyphenated). This identifier is used to refer to
 the workflow through the API.

#### `docker_image` **[string]**:
The Docker image that should be executed.

#### `docker_args` **[string]**
The list of arguments passed to the Docker container.

This list should only contain strings. Any occurrences of the `{}` placeholder argument will be
replaced with the current partition date or datehour. Note that it must be quoted in the yaml file
in order not to be interpreted as an object.

Example arguments for the supported schedule values:
```
- hourly - 2016-04-01T14, 2016-04-01T15, ... (UTC hours)
- daily  - 2016-04-01,    2016-04-02,    ...
- weekly - 2016-04-04,    2016-04-11,    ... (Mondays)
```

#### `schedule` **[string]**
How often the workflow should be triggered and what the `{}` placeholder will be replaced with in
`docker_args`.

Supports [cron] syntax, along with a set of human readable aliases:
```
@hourly,   hourly   = 0 * * * *
@daily,    daily    = 0 0 * * *
@weekly,   weekly   = 0 0 * * MON
@monthly,  monthly  = 0 0 1 * *
@yearly,   yearly   = 0 0 1 1 *
@annually, annually = 0 0 1 1 *
```

#### `offset` **[string]**
An [ISO 8601 Duration] specification for offsetting the cron schedule.

This is useful for when setting up a schedule that needs to be offset in time relative to the
schedule timestamps. For instance, an hourly schedule that needs to process a bucket of data
for each hour will not be able to run until at the end of that hour. We can then use an offset
value of `PT1H`. The injected placeholder would reflect the logical time of the schedule (00,
01, 02, ...), but it would actually run one hour later (01, 02, 03, ...). This is specially
useful for irregular schedules.

In fact, it is so common that we need to use a "last hour" parameter in jobs that we've set the
default offset for all the well known (aliased) schedules to +1 period. E.g for an `@hourly`
schedule, the default offset is `PT1H`, and for a `@daily` schedule the offset is `P1D`

Example: a job needs to run daily at 2 AM but the partition argument needs to be midnight

```yaml
schedule: '@daily'
offset: P1DT2H
```

At 2017-06-30T02 the execution for 2017-06-29 will be triggered.

#### `secret` **[secret]**
Secret is used to mount keys stored in [Kubernetes Secrets] into the container.

* `.name` **[string]**

  Name of the secret stored in Kubernetes

* `.mount_path` **[string]**

  Where the keys of the secret will be appearing in the container

#### `service_account` **[email address]**
The [Service Account] email address belonging to a project in [Google Cloud Platform].

If the workflow intends to use keys of a [Service Account],
Styx will create both JSON and p12 keys for the specified `service_account`, rotate keys on daily basis,
and garbage collect unused keys older than 48h.

Styx stores the created keys in [Kubernetes Secrets] and mounts them under `/etc/styx-wf-sa-keys/`
in the container.

Styx injects an environment variable to the container named as `GOOGLE_APPLICATION_CREDENTIALS` pointing
to the JSON key file.

It is allowed and perfectly fine to have both `secret` and `service_account` configured for a workflow.
However users need to make sure `secret.mount_path` doesn't point to `/etc/styx-wf-sa-keys/`; otherwise
Styx will refuse to trigger the workflow.

In order for Styx to be able to create/delete keys for the `service_account` of a workflow,
the [Service Account] that Styx itself runs as should be granted `Service Account Key Admin`
role for the `service_account` of the workflow. This can be done by following
[Granting Roles to Service Accounts].

### Triggering and executions

Each time a Workflow Schedule is triggered, Styx will treat that trigger as a first class entity.
Each Trigger will have at least one Execution which can potentially take a long time to execute. If
another Trigger happens during this time, both triggers will be active, each with one running
container. Because Styx treats each Trigger individually, it can ensure that each one of them
complete successfully.

Styx does not assume anything about what is executed in the container, it only cares about the exit
code. Any execution returning a non-zero exit code will either cause a re-try to be scheduled, or be
interpreted as a permanent failure of the workflow instance. For detailed description of exit codes,
please refer to **Workflow state graph** section in [Styx design].

### Injected environment variables

For each execution, Styx will inject a set of environment variables into the Docker container.

| Variable Name | Description |
|---|---|
| `STYX_COMPONENT_ID` | The component id of the workflow. This will be the filename of the file which defines the workflow schedule. |
| `STYX_WORKFLOW_ID` | The workflow id of the workflow. This is the `id` field specified in the workflow schedule. |
| `STYX_PARAMETER` | The parameter argument. See section about `docker_args` above. |
| `STYX_SERVICE_ACCOUNT` | The service account. |
| `STYX_COMMIT_SHA` | The commit-sha of the workflow. |
| `STYX_DOCKER_ARGS` | The arguments passed to the container. |
| `STYX_DOCKER_IMAGE` | The docker image. |
| `STYX_TRIGGER_ID` | The ID of the trigger. |
| `STYX_TRIGGER_TYPE` | The type of the trigger. Possible values are: `natural`, `adhoc`, `backfill` and `unknown` |
| `STYX_EXECUTION_ID` | A unique identifier for the execution. This is the execution id used to identify execution attempts of a trigger. |
| `STYX_EXECUTION_COUNTER` | **to be implemented** - A counter indicating which execution this is. Goes from 0..N per trigger. |


## Development

### Code Coverage

An aggregate code coverage report for the entire project is created by the `report` submodule.

```bash
> mvn clean verify
> open report/target/site/jacoco-aggregate/index.html
```

CircleCI builds submit code coverage reports to [codecov.io]. In addition, the aggregate
JaCoCo report can be viewed under the Artifacts tab in the CircleCI build view.

---

This project adheres to the [Open Code of Conduct][code-of-conduct]. By participating, you are
expected to honor this code.

[Styx design]: doc/design-overview.md
[External services]: doc/external-services.md
[Kubernetes]: http://kubernetes.io/
[Apollo]: https://spotify.github.io/apollo/
[Luigi]: https://github.com/spotify/luigi
[code-of-conduct]: https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md
[cron]: https://en.wikipedia.org/wiki/Cron
[ISO 8601 Duration]: https://en.wikipedia.org/wiki/ISO_8601#Durations
[Kubernetes Secrets]: https://kubernetes.io/docs/concepts/configuration/secret/
[Service Account]: https://cloud.google.com/compute/docs/access/service-accounts
[Google Cloud Platform]: https://cloud.google.com/
[Granting Roles to Service Accounts]: https://cloud.google.com/iam/docs/granting-roles-to-service-accounts
[codecov.io]: https://codecov.io/gh/spotify/styx

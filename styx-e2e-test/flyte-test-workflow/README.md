---
**Note**

Everything has been deployed to flyte already under 04e346ca5f43fc2778259d77a2d9f64ce42b2a27.
We only need to update the workflow unless it is really necessary. Then you should follow the update steps below.
---

### Redeploy

In case the already deployed resources got wiped out, run these commands to redeploy:

```
$ kubectl port-forward svc/envoy 8089:80 -n flyte

$ flyte-cli -h localhost:8089 -i \
  register-files -p flytesnacks -d development -v "$(git rev-parse HEAD)" \
  _pb_output/*
```

### Update

#### Setup
Install `flytekit` which will install: `pyflyte`, `flyte-cli` and `flytekit_build_image.sh`:

```
$ virtualenv .venv -p python3.8
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

#### Build and push image
Build and push image:

```
$ REGISTRY=eu.gcr.io/styx-oss-test flytekit_build_image.sh .
```

To only build, do:

```
$ flytekit_build_image.sh .
```

#### Port forwarding to flyteadmin in the test cluster
Access to flyteadmin is needed to deploy the workflow. You can create port forwarding to flyteadmin with this command
```
$ kubectl port-forward svc/envoy 8089:80 -n flyte
```

#### Deploy to Flyte
Deploys workflows, tasks, launch plans and image to Flyte.

```
$ docker run \
  -v "$(pwd)/_pb_output:/tmp/output" \
  "eu.gcr.io/styx-oss-test/flyte-test-workflow:$(git rev-parse HEAD)" \
  pyflyte --config ./sandbox.config serialize workflows -f /tmp/output

$ flyte-cli -h localhost:8089 -i \
  register-files -p flytesnacks -d development -v "$(git rev-parse HEAD)"
  _pb_output/*
```

#### Test the workflow
Try to execute the launch plan of workflow once to make sure it work:

```
$ flyte-cli -h localhost:8089 -i \
  -p flytesnacks -d development execute-launch-plan \
  -u "lp:flytesnacks:development:morning_greeting:$(git rev-parse HEAD)" \
  -w \
  -r default \
  -- styx_parameter=2020-01-01T00Z styx_trigger_type=x styx_trigger_id=x styx_execution_id=x styx_workflow_id=x 
```

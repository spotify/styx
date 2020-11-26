---
**Note**

Everything has been deployed to flyte already under b495f8671b8fc2da9e51acd5803a26d52e18795a.
We only need to update the workflow unless it is really necessary. Then you should follow the steps here.
---

#### Redeploy

In case the already deployed resources got wiped out, run these commands to redeploy:

```
kubectl -n flyte port-forward deployment/flyteadmin 8089:8089

docker run --network host \
-e FLYTE_PLATFORM_URL='host.docker.internal:8089' \
"eu.gcr.io/styx-oss-test/flyte-test-workflow:b495f8671b8fc2da9e51acd5803a26d52e18795a" \
pyflyte -p flytesnacks -d development -c sandbox.config register workflows
```

#### Setup
Install `flytekit` which will install: pyflyte, flyte-cli and flytekit_build_image.sh 
```
$ virtualenv .venv -p python3.6
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

#### Build and push image
Build and push image,

```
$ TAG=$(git log --format='%H' -n 1) REGISTRY=eu.gcr.io/styx-oss-test \
 flytekit_build_image.sh .
```

To only build, do,
```
$ flytekit_build_image.sh .
```

#### Port forwarding to flyteadmin in the test cluster
Access to flyteadmin is needed to deploy the workflow. You can create port forwarding to flyteadmin with this command
```
kubectl port-forward -n flyte port-forward deployment/flyteadmin 8089:8089
```

#### Deploy to flyte 
Deploys workflows, tasks, launch plans and image to flyte.

```
docker run --network host \
-e FLYTE_PLATFORM_URL='host.docker.internal:8089' \
"eu.gcr.io/styx-oss-test/flyte-test-workflow:$(git log --format='%H' -n 1)" \
pyflyte -p flytesnacks -d development -c sandbox.config register workflows
```

#### Test the workflow
Try to execute the lp of workflow once to make sure it work
```
flyte-cli -h 127.0.0.1:8089 -i \
-p flytesnacks -d development execute-launch-plan -r aa \
 -u "lp:flytesnacks:development:workflows.hello_world_launch_plan.lp:$(git log --format='%H' -n 1)"
```

To check if the execution worked
```
flyte-cli -h $FLYTE_ADMIN_ADDRESS -i -p flytesnacks -d development\
  get-execution  -u ex:flytesnacks:development:$EXECUTION_ID  
```

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

#### Deploy to flyte
Deploys workflows, tasks, launch plans and image to flyte.

You need to go to gcloud console to change the flyte admin service deployment to use LoadBalancer and get the IP of `$FLYTE_ADMIN_ADDRESS` 
```
docker run --network host -e FLYTE_PLATFORM_URL='$FLYTE_ADMIN_ADDRESS' "eu.gcr.io/styx-oss-test/flyte-test-workflow
:$(git log --format='%H' -n 1)" pyflyte -p flytesnacks -d development -c sandbox.config register workflows
```

####Test the workflow
Try to execute the lp of workflow once to make sure it work
```
flyte-cli -h $FLYTE_ADMIN_ADDRESS -i -p flytesnacks -d development execute-launch-plan -r aa \                                       
  -u lp:flytesnacks:development:workflows.hello_world_workflow.WorkflowHelloWorld:$(git log --format='%H' -n 1)
```

To check if the execution worked
```
flyte-cli -h $FLYTE_ADMIN_ADDRESS -i -p flytesnacks -d development  get-execution  -u ex:flytesnacks:development
:$EXECUTION_ID  
```
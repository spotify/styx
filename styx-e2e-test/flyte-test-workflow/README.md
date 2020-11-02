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


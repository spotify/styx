from flytekit.sdk.tasks import  python_task


@python_task(cache_version="1", cache=False)
def print_hello_world(wf_params):
    if wf_params and wf_params.logging:
        wf_params.logging.info("Hello world!!")

from flytekit.sdk.workflow import workflow_class
from workflows.hello_world_task import print_hello_world


@workflow_class()
class WorkflowHelloWorld:
    hello = print_hello_world()
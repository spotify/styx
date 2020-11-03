from flytekit.sdk.workflow import workflow_class
from workflows.hello_world_task import print_hello_world


@workflow_class(disable_default_launch_plan=True)
class WorkflowHelloWorld:
    hello = print_hello_world()


WorkflowHelloWorld.create_launch_plan(kubernetes_service_account="e2e-test-sa")

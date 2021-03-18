from datetime import datetime
from flytekit import LaunchPlan, task, workflow
from flytekit.models.common import AuthRole

@task
def print_hello_world() -> str:
    print("Hello, World")

    return "Hello"


@workflow
def hello_world(
  styx_parameter: datetime,
  styx_execution_id: str,
  styx_trigger_id: str,
  styx_trigger_type: str,
  styx_workflow_id: str
) -> str:
    hello = print_hello_world()

    return hello


lp = LaunchPlan.create("morning_greeting", hello_world, auth_role=AuthRole(kubernetes_service_account="e2e-test-sa"))

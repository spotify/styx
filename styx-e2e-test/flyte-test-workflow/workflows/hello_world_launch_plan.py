from workflows.hello_world_workflow import WorkflowHelloWorld

lp = WorkflowHelloWorld.create_launch_plan(kubernetes_service_account="e2e-test-sa")

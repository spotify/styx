# Styx Audit Logging

Styx performs audit logging of events that might be of interest when monitoring the security and investigating potential
unauthorized access to Styx and/or the underlying infrastructure.

| Message  | Description |
| ------------- | ------------- |
| `[AUDIT] {} {} by {} with headers {} parameters {} and payload {}`  | Styx received a non-GET http request. These requests might be create, making changes to or deleting workflows or other Styx resources.  |
| `[AUDIT] Got pod without workflow instance annotation {}", podName);` | Styx detected a Kubernetes Pod without a Styx annotation. It was likely not created by Styx and indicates that someone is accessing the Styx Kubernetes cluster directly. |
| `[AUDIT] Workflow {} refers to secret {} storing keys of {}",` | Styx is executing a workflow configured with a GCP Service Account. |
| `[AUDIT] Workflow {} refers to non-existent service account {}", workflowId, serviceAccount);` | Execution was denied for a workflow configured to use a non-existent GCP Service Account. |
| `[AUDIT] Service account keys have been deleted for {}, recreating", serviceAccount);` | Styx detected that the keys for a GCP Service Account have been deleted and are creating new keys for the account. |
| `[AUDIT] Failed to create keys for {}", serviceAccount, e);` | Styx failed to create keys for a GCP Service Account. |
| `[AUDIT] Secret {} created to store keys of {} referred by workflow {}, jsonKey: {}, p12Key: {}",` | Styx created a Kubernetes secret to store GCP Service Account Keys for a workflow. |
| `[AUDIT] Deleting service account key: {}", jsonKeyName);` | Styx is deleting the JSON key for a GCP Service Account as part of key rotation. |
| `[AUDIT] Deleting service account key: {}", p12KeyName);` | Styx is deleting the P12 key for a GCP Service Account as part of key rotation. |
| `[AUDIT] Deleting service account {} secret {}", serviceAcount, name);` | Styx is deleting the Kubernetes secret storing GCP Service Account Keys as part of key rotation. |
| `[AUDIT] Failed to delete service account {} keys and/or secret {}",` | Styx failed to delete either GCP Service Account keys or the Kubernetes secret storing the keys. |
| `[AUDIT] Permission denied when trying to delete unused service account key {}",` | Styx failed to delete a GCP Service Account Key due to a permission denied error. This indicates that access to this service account was removed for the Styx service account. |


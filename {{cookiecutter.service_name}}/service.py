# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
import pathlib
from urllib.parse import urlparse
from uuid import uuid4

try:
    import zoo
except ImportError:

    class ZooStub(object):
        def __init__(self):
            self.SERVICE_SUCCEEDED = 3
            self.SERVICE_FAILED = 4

        def update_status(self, conf, progress):
            print(f"Status {progress}")

        def _(self, message):
            print(f"invoked _ with {message}")

    zoo = ZooStub()

import json
import os
import sys
from urllib.parse import urlparse

print(f"python path = {sys.path}")
print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)

import boto3  # noqa: F401
import botocore
import jwt
import requests
import yaml
from botocore.exceptions import ClientError
from loguru import logger
from pystac import read_file
from pystac.stac_io import DefaultStacIO, StacIO
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner
from botocore.client import Config
from pystac.item_collection import ItemCollection

# For DEBUG
import traceback

logger.remove()
logger.add(sys.stderr, level="INFO")

# #####################################################################################################
from dataclasses import dataclass, field
import logging
import json
from typing import Any, Optional
from uuid import uuid4

from kubernetes import client, config, watch
import yaml

@dataclass
class WorkflowStorageCredentials:
    url: str
    access_key: str
    secret_key: str
    insecure: bool = True

    def __post_init__(self):
        parsed_url = urlparse(self.url)
        self.url = parsed_url.netloc
        storage_protocol = parsed_url.scheme
        self.insecure = storage_protocol == "http"


@dataclass
class WorkflowConfig:
    namespace: str
    workflow_template: Optional[str] = field(default=None)
    workflow_id: Optional[str] = field(default=None)
    workflow_parameters: list[dict] = field(default_factory=list)
    storage_credentials: Optional[WorkflowStorageCredentials] = field(default=None)


class ArgoWorkflow:
    def __init__(self, workflow_config: WorkflowConfig, conf: dict):
        self.workflow_config = workflow_config
        self.conf = conf
        self.job_namespace: Optional[str] = None
        self.workflow_manifest = None

        # Load the kube config from the default location
        logger.info("Loading kube config")
        # config.load_kube_config()
        config.load_config()

        # Create a new client
        logger.info("Creating a new K8s client")
        self.v1 = client.CoreV1Api()
        self.rbac_v1 = client.RbacAuthorizationV1Api()
        self.custom_api = client.CustomObjectsApi()

    def _create_job_namespace(self):
        # Create the namespace
        if self.workflow_config.workflow_id is None:
            raise ValueError("workflow_id is required")

        self.job_namespace = f"{self.workflow_config.namespace}-{self.workflow_config.workflow_id}".lower()
        logger.info(f"Creating namespace: {self.job_namespace}")
        namespace_body = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=self.job_namespace)
        )
        self.v1.create_namespace(body=namespace_body)

    # Create storage secret on K8s
    def _create_job_secret(self):
        logger.info(f"Creating storage secrets for namespace: {self.job_namespace}")
        secret_body = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name="storage-credentials"
            ),  # this name needs to match the configuration for the workflow-controller (workflow-controller-configmap)
            string_data={
                "access-key": self.workflow_config.storage_credentials.access_key,
                "secret-key": self.workflow_config.storage_credentials.secret_key,
            },
        )
        self.v1.create_namespaced_secret(namespace=self.job_namespace, body=secret_body)

    def _create_artifact_repository_configmap(self):
        logger.info(f"Creating artifact repository configmap for namespace: {self.job_namespace}")
        # Define ConfigMap metadata
        metadata = client.V1ObjectMeta(
            name="artifact-repository",
            annotations={
                "workflows.argoproj.io/default-artifact-repository": "default-v1-s3-artifact-repository"
            }
        )

        # Define ConfigMap data
        data = {
            'default-v1-s3-artifact-repository': f"""
            archiveLogs: true
            s3:
                bucket: {self.workflow_config.namespace}
                endpoint: {self.workflow_config.storage_credentials.url}
                insecure: {str(self.workflow_config.storage_credentials.insecure).lower()}
                accessKeySecret:
                    name: storage-credentials
                    key: access-key
                secretKeySecret:
                    name: storage-credentials
                    key: secret-key
            """
        }

        # Create ConfigMap object
        config_map = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=metadata,
            data=data
        )

        # Create ConfigMap
        self.v1.create_namespaced_config_map(
            namespace=self.job_namespace,
            body=config_map
        )

    # Create the Role
    def _create_job_role(self):
        logger.info(f"Creating role for namespace: {self.job_namespace}")

        # artifactGC role
        artifact_gc_policy_rule = client.V1PolicyRule(
            api_groups=["argoproj.io"],
            resources=["workflows", "workflows/finalizers", "workflowartifactgctasks", "workflowartifactgctasks/status"],
            verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
        )

        pods_policy_rule = client.V1PolicyRule(
            api_groups=[""],
            resources=["pods"],
            verbs=["get", "list", "watch", "create", "update", "patch", "delete"],
        )

        role_body = client.V1Role(
            metadata=client.V1ObjectMeta(name="pod-patcher"),
            rules=[
                pods_policy_rule,
                artifact_gc_policy_rule
            ],
        )

        self.rbac_v1.create_namespaced_role(
            namespace=self.job_namespace, body=role_body
        )

    # Create the RoleBinding
    def _create_job_role_binding(self):
        logger.info(f"Creating role binding for namespace: {self.job_namespace}")
        role_binding_body = client.V1RoleBinding(
            metadata=client.V1ObjectMeta(name="pod-patcher-binding"),
            subjects=[
                {
                    "kind": "ServiceAccount",
                    "name": "default",
                    "namespace": self.job_namespace,
                }
            ],
            role_ref=client.V1RoleRef(
                api_group="rbac.authorization.k8s.io", kind="Role", name="pod-patcher"
            ),
        )
        self.rbac_v1.create_namespaced_role_binding(
            namespace=self.job_namespace, body=role_binding_body
        )

    def _save_template_job_namespace(self):
        template_manifest = self.workflow_manifest
        logger.info(f"template_manifest = {json.dumps(template_manifest, indent=2)}")
        template_manifest["metadata"]["name"] = template_manifest["metadata"]["name"].lower()
        template_manifest["metadata"]["namespace"] = self.job_namespace
        template_manifest["metadata"]["resourceVersion"] = None
        self.save_workflow_template(
            template_manifest=template_manifest, namespace=self.job_namespace
        )

    def load_workflow_template(self):
        try:
            if self.workflow_config.workflow_template is None:
                raise ValueError("workflow_template is required")

            # Get the template
            self.workflow_manifest = self.custom_api.get_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.workflow_config.namespace,
                plural="workflowtemplates",
                name=self.workflow_config.workflow_template,
            )

        except Exception as e:
            logger.error(f"Error loading template: {e}")
            raise e

    def save_workflow_template(
        self, template_manifest: dict = None, namespace: Optional[str] = None
    ):
        try:
            # Create the template
            namespace = namespace or self.workflow_config.namespace
            workflow_template_name = template_manifest["metadata"]["name"]
            logger.info(
                f"Creating workflow template {workflow_template_name} on namespace {namespace}"
            )

            self.custom_api.create_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=namespace,
                plural="workflowtemplates",
                body=template_manifest,
            )
            logger.info(f"Workflow template {workflow_template_name} created successfully")
        except Exception as e:
            logger.error(f"Error saving template: {e}")
            raise e

    # Submit the workflow
    def _submit_workflow(self):
        if self.workflow_config.workflow_id is None:
            raise ValueError("workflow_id is required")

        workflow_manifest = {
            "apiVersion": "argoproj.io/v1alpha1",
            "kind": "Workflow",
            "metadata": {
                "name": f"{self.workflow_config.workflow_id}".lower()
            },
            "spec": {
                "workflowTemplateRef": {
                    "name": self.workflow_manifest["metadata"]["name"],
                    "namespace": self.workflow_manifest["metadata"]["namespace"],
                },
                "arguments": {
                    "parameters": self.workflow_config.workflow_parameters  # Add parameters if provided
                },
            },
        }

        workflow = self.custom_api.create_namespaced_custom_object(
            group="argoproj.io",
            version="v1alpha1",
            namespace=self.job_namespace,
            plural="workflows",
            body=workflow_manifest,
        )

        return workflow
    
    def _update_status(self, progress: int, message: str = None) -> None:
        """updates the execution progress (%) and provides an optional message"""
        if message:
            self.conf["lenv"]["message"] = message

        zoo.update_status(self.conf, progress)

    # Monitor the workflow execution
    def monitor_workflow(self, workflow: dict):
        logger.info(f"Monitoring workflow {workflow['metadata']['name']}")

        if self.job_namespace is None:
            raise ValueError("job_namespace is required")

        w = watch.Watch()
        workflow_stream = w.stream(
            self.custom_api.list_namespaced_custom_object,
            group="argoproj.io",
            version="v1alpha1",
            namespace=self.job_namespace,
            plural="workflows",
            field_selector=f"metadata.name={workflow['metadata']['name']}",
        )

        phase_data = None
        self._update_status(10, "Workflow started")
        for data in workflow_stream:
            event_data = data["type"]
            workflow_name = data["object"]["metadata"]["name"]
            phase_data = data["object"].get("status", {}).get("phase")
            progress_data = data["object"].get("status", {}).get("progress")

            # Stop watching if the workflow has reached a terminal state
            if phase_data in ["Succeeded", "Failed", "Error"]:
                logger.info(
                    f"Workflow {workflow_name} has reached a terminal state: {phase_data}"
                )

                # need to save this on the logs?
                logger.info(json.dumps(data, indent=2))
                self._update_status(100, phase_data)
                w.stop()

            # print(data)
            logger.info(
                f"Event: {event_data}, Workflow: {workflow_name}, Phase: {phase_data}, Progress: {progress_data}"
            )

        logger.info("Workflow monitoring complete")
        
        if phase_data == "Succeeded":
            return zoo.SERVICE_SUCCEEDED
        else:
            return zoo.SERVICE_FAILED

    def run(self):
        # Load the workflow template
        logger.info(f"Loading workflow template: {self.workflow_config.workflow_template}")
        self.load_workflow_template()

        # Create the namespace, access key, and secret key
        logger.info("Creating namespace, roles, and storage secrets")
        self._create_job_namespace()
        self._create_job_secret()
        self._create_artifact_repository_configmap()
        self._create_job_role()
        self._create_job_role_binding()

        # Template workflow needs to be on the same namespace as the job
        self._save_template_job_namespace()

        workflow = self._submit_workflow()
        exit_status = self.monitor_workflow(workflow)
        return exit_status

    def run_workflow_from_file(self, workflow_file: dict):
        self.workflow_manifest = workflow_file
        # Create the namespace, access key, and secret key
        logger.info("Creating namespace, roles, and storage secrets")
        self._create_job_namespace()
        self._create_job_secret()
        self._create_artifact_repository_configmap()
        self._create_job_role()
        self._create_job_role_binding()

        # Template workflow needs to be on the same namespace as the job
        self._save_template_job_namespace()

        workflow = self._submit_workflow()
        exit_status = self.monitor_workflow(workflow)
        return exit_status
    
    def save_workflow_logs(self, log_filename="logs.txt"):
        try:
            logger.info(f"Getting logs for workflow {self.workflow_config.workflow_id} in namespace {self.job_namespace}")
            # list pods for namespace
            pods = self.v1.list_namespaced_pod(namespace=self.job_namespace)

            logger.info(f"Saving logs to {log_filename}")
            with open(log_filename, 'w') as f:
                for pod in pods.items:
                    # only get logs from pods that belong to the workflow
                    if self.workflow_config.workflow_id not in pod.metadata.name:
                        continue
                    
                    logger.info(f"Getting logs for pod {pod.metadata.name}")
                    f.write(f"{'='*80}\n")
                    f.write(f"Logs for pod {pod.metadata.name}:\n")
                    f.write(f"{'='*80}\n")
                    for container in pod.spec.containers:
                        f.write(f"Container {container.name}:\n")
                        f.write(self.v1.read_namespaced_pod_log(
                            name=pod.metadata.name, 
                            namespace=self.job_namespace,
                            container=container.name
                        ))
                
                logger.info(f"Logs saved to {log_filename}")
                f.write(f"\n{'='*80}\n")

            #
            if self.conf is not None:
                servicesLogs = {
                    "url": os.path.join(self.conf['main']['tmpUrl'], self.job_namespace, os.path.basename(log_filename)),
                    "title": f"Process execution log {os.path.basename(log_filename)}",
                    "rel": "related",
                }

                if not self.conf.get("service_logs"):
                    self.conf["service_logs"] = {}

                for key in servicesLogs.keys():
                    self.conf["service_logs"][key] = servicesLogs[key]

                self.conf["service_logs"]["length"] = "1"
                

        except Exception as e:
            logger.error(f"Error getting logs: {e}")
            raise e
        
    def delete_workflow(self):
        try:
            self.v1.delete_namespace(name=self.job_namespace)
            logger.info(f"Namespace {self.job_namespace} deleted.")
        except Exception as e:
            logger.error(f"Error deleting namespace {self.job_namespace}: {e}")
# #####################################################################################################


class CustomStacIO(DefaultStacIO):
    """Custom STAC IO class that uses boto3 to read from S3."""

    def __init__(self):
        self.session = botocore.session.Session()
        self.s3_client = self.session.create_client(
            service_name="s3",
            region_name=os.environ.get("AWS_REGION"),
            endpoint_url=os.environ.get("AWS_S3_ENDPOINT"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            verify=True,
            use_ssl=True,
            config=Config(s3={"addressing_style": "path", "signature_version": "s3v4"}),
        )

    def read_text(self, source, *args, **kwargs):
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            return (
                self.s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path[1:])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest, txt, *args, **kwargs):
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            self.s3_client.put_object(
                Body=txt.encode("UTF-8"),
                Bucket=parsed.netloc,
                Key=parsed.path[1:],
                ContentType="application/geo+json",
            )
        else:
            super().write_text(dest, txt, *args, **kwargs)


StacIO.set_default(CustomStacIO)


class EoepcaCalrissianRunnerExecutionHandler(ExecutionHandler):
    def __init__(self, conf, inputs):
        super().__init__()
        logger.info("Init EoepcaCalrissianRunnerExecutionHandler")
        
        self.conf = conf
        self.inputs = inputs

        # #####################################################################################################
        # TODO: check why we need this
        # I think to be able to use kubectl. It calls a service kubeproxy on zoo namespace
        # #####################################################################################################
        self.http_proxy_env = os.environ.get("HTTP_PROXY", None)

        # Decode the JWT token
        auth_env = self.conf.get("auth_env", {})
        self.ades_rx_token = auth_env.get("jwt", "")
        self.decoded_token = self._decode_jwt(self.ades_rx_token)
        
        # get the user name from the token
        logger.info(f"get the user name from the token : {self.decoded_token}")
        if self.ades_rx_token:
            self.username = self.get_user_name(self.decoded_token)
            logger.info(f"User name: {self.username}")

        # logic to handle internal (subscription) vs external requests
        if self.decoded_token.get("job_type") == "subscription":
            # internal submission
            self.workspace_url = self.decoded_token.get("workspace_url")
            self.workspace = self.decoded_token.get("workspace")
            self.use_workspace = self.workspace != "global"
            self.username = self.decoded_token.get("preferred_username")
            self.registration_api_url = eoepca.get("registration_api_url", "")
            self.resource_catalog_api_url = eoepca.get("resource_catalog_api_url", "")
            logger.info(f"Internal submission for user {self.username} in workspace {self.workspace}")
        else:
            # external/user submission
            eoepca = self.conf.get("eoepca", {})

            # "eoepca" field comes from main.cfg file (stored on zoo-project-dru-zoo-project-config configmap)
            # "eoepca": {
            #     "domain": "mkube.dec.earthdaily.com",
            #     "workspace_url": "https://workspace-api.mkube.dec.earthdaily.com",
            #     "workspace_prefix": "ws"
            # }
            self.workspace_url = eoepca.get("workspace_url", "")
            self.workspace_prefix = eoepca.get("workspace_prefix", "")
            self.registration_api_url = eoepca.get("registration_api_url", "")
            self.resource_catalog_api_url = eoepca.get("resource_catalog_api_url", "")
            
            # Check if the workspace settings are available
            if self.workspace_url and self.workspace_prefix:
                self.use_workspace = True
                self.workspace = f"{self.workspace_prefix}-{self.username}"
                logger.info(f"External submission for user {self.username} in workspace {self.workspace}")
            else:
                self.use_workspace = False
                logger.info(f"External submission for user {self.username} (no workspace settings available)")

            
        self.feature_collection = None

        # This function modifies the conf object
        self.init_config_defaults(self.conf)

    def _decode_jwt(self, token: str):
        try:
            return jwt.decode(token, options={"verify_signature": False})
        except Exception as e:
            logger.error(f"Error decoding JWT token: {e}")
            return None

    def pre_execution_hook(self):
        try:
            logger.info("Pre execution hook")
            
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz PRE-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")

            if self.use_workspace:
                logger.info("Using Workspace settings")
                logger.info("Lookup storage details in Workspace")

                # Workspace API endpoint
                uri_for_request = f"workspaces/{self.workspace}"
                logger.info(f"uri_for_request = {uri_for_request}")

                workspace_api_endpoint = os.path.join(self.workspace_url, uri_for_request)
                logger.info(f"Using Workspace API endpoint {workspace_api_endpoint}")

                # Request: Get Workspace Details
                # #####################################################################################################
                # As we are using internal call to workspace, no Authorization is needed.
                # We can keep the code as it is, but no information is passed to workspace-api through token
                # Workspace-api is used only to get Storage credentials
                # Need to configure default storage credentials for the public folder
                # #####################################################################################################
                headers = {
                    "accept": "application/json",
                    "Authorization": f"Bearer {self.ades_rx_token}",
                }

                logger.info("Get Workspace details")
                get_workspace_details_response = requests.get(workspace_api_endpoint, headers=headers)

                # GOOD response from Workspace API - use the details
                if get_workspace_details_response.ok:
                    workspace_response = get_workspace_details_response.json()
                    logger.info(f"Workspace response: {json.dumps(workspace_response, indent=4)}")

                    logger.info("Set user bucket settings")

                    storage_credentials = workspace_response["storage"]["credentials"]
                    logger.info(f"Storage credentials: {json.dumps(storage_credentials, indent=4)}")

                    logger.info("Adding storage credentials to the additional parameters")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = storage_credentials.get("endpoint")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = storage_credentials.get("access")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = storage_credentials.get("secret")
                    self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = storage_credentials.get("region")
                    self.conf["additional_parameters"]["STAGEOUT_OUTPUT"] = storage_credentials.get("bucketname")

                # BAD response from Workspace API - fallback to the 'pre-configured storage details'
                else:
                    logger.error("Problem connecting with the Workspace API")
                    logger.info(f"  Response code = {get_workspace_details_response.status_code}")
                    logger.info(f"  Response text = \n{get_workspace_details_response.text}")
                    self.use_workspace = False
                    logger.info("Using pre-configured storage details")
            else:
                logger.info("Using pre-configured storage details")

            lenv = self.conf.get("lenv", {})
            logger.info(f"lenv = {lenv}")
            # ###########################################################################
            # TODO: check where this is used 
            # -> Additional Parameters are passed to the CWL 
            # Note:
            # instead of using a random number for collection_id, we try to use 
            # collection name from input. Use usid if collection name is not available
            # ###########################################################################
            collection_id = self.inputs.get("collection_id", {}).get("value")
            if collection_id is None:
                collection_id = lenv.get("usid", "")
            
            process_job_id = lenv.get("usid", str(uuid4()))
            process = os.path.join("processing-results", process_job_id)
            
            logger.info(f"collection_id = {collection_id}")
            logger.info(f"process = {process}")
            
            self.conf["additional_parameters"]["collection_id"] = collection_id
            self.conf["additional_parameters"]["process"] = process

        except Exception as e:
            logger.error("ERROR in pre_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)
        
        finally:
            self.restore_http_proxy_env()

    def post_execution_hook(self, log, output, usage_report, tool_logs):
        try:
            logger.info("Post execution hook")
            self.unset_http_proxy_env()

            # DEBUG
            # logger.info(f"zzz POST-HOOK - config...\n{json.dumps(self.conf, indent=2)}\n")

            logger.info("Set user bucket settings")
            os.environ["AWS_S3_ENDPOINT"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
            os.environ["AWS_ACCESS_KEY_ID"] = self.conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"]
            os.environ["AWS_REGION"] = self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]
            
            logger.info(f"Setting env AWS_S3_ENDPOINT = {os.environ['AWS_S3_ENDPOINT']}")
            logger.info(f"Setting env AWS_ACCESS_KEY_ID = {os.environ['AWS_ACCESS_KEY_ID']}")
            logger.info(f"Setting env AWS_SECRET_ACCESS_KEY = {os.environ['AWS_SECRET_ACCESS_KEY']}")
            logger.info(f"Setting env AWS_REGION = {os.environ['AWS_REGION']}")
    

            logger.info("Setting custom STAC IO class")
            StacIO.set_default(CustomStacIO)

            # output["StacCatalogUri"] = s3://ws-bob/processing-results/19b85634-080a-11ef-89d1-0242ac11002f/catalog.json
            logger.info(f"Read catalog => STAC Catalog URI: {output['StacCatalogUri']}")
            try:
                s3_path = output["StacCatalogUri"]
                if s3_path.count("s3://")==0:
                    s3_path = "s3://" + s3_path
                cat = read_file( s3_path )
            except Exception as e:
                logger.error("ERROR reading catalog")
                logger.error(f"Exception: {e}")

            collection_id = self.conf["additional_parameters"]["collection_id"]
            logger.info(f"Create collection with ID {collection_id}")
            
            collection = None
            try:
                collection = next(cat.get_all_collections())
                logger.info(f"Got collection {json.dumps(collection.to_dict())} from outputs")
            except:
                try:
                    items=cat.get_all_items()
                    itemFinal=[]
                    for i in items:
                        for a in i.assets.keys():
                            cDict=i.assets[a].to_dict()
                            cDict["storage:platform"]="EOEPCA"
                            cDict["storage:requester_pays"]=False
                            cDict["storage:tier"]="Standard"
                            cDict["storage:region"]=self.conf["additional_parameters"]["STAGEOUT_AWS_REGION"]
                            cDict["storage:endpoint"]=self.conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"]
                            i.assets[a]=i.assets[a].from_dict(cDict)
                        i.collection_id=collection_id
                        itemFinal+=[i.clone()]
                    collection = ItemCollection(items=itemFinal)
                    logger.info("Created collection from items")
                except Exception as e:
                    logger.error("ERROR creating collection")
                    logger.error(f"Exception: {e}"+str(e))
            
            # Trap the case of no output collection
            if collection is None:
                logger.error("ABORT: The output collection is empty")
                self.feature_collection = json.dumps({}, indent=2)
                return

            collection_dict=collection.to_dict()
            collection_dict["id"]=collection_id

            # Set the feature collection to be returned
            self.feature_collection = json.dumps(collection_dict, indent=2)
            logger.info(f"self.feature_collection = {self.feature_collection}")

            # Register with the workspace
            logger.info(f"Register with the workspace? {self.use_workspace}")
            if self.use_workspace:
                logger.info(f"Register collection in workspace {self.workspace}")
                headers = {
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.ades_rx_token}",
                }
                api_endpoint = f"{self.workspace_url}/workspaces/{self.workspace}"
                logger.info(f"api_endpoint: {api_endpoint}")
                logger.info(f"payload = {collection_dict}")
                logger.info(f"headers = {headers}")
                r = requests.post(
                    f"{api_endpoint}/register-json",
                    json=collection_dict,
                    headers=headers,
                )
                logger.info(f"Register collection response: {r.status_code}")

                # TODO pool the catalog until the collection is available
                #self.feature_collection = requests.get(
                #    f"{api_endpoint}/collections/{collection.id}", headers=headers
                #).json()
            
                logger.info(f"Register processing results to collection: {collection_id}")
                stac_catalog = {"type": "stac-item", "url": output['StacCatalogUri'].replace("/catalog.json", "")}
                logger.info(f"stac_catalog = {stac_catalog}")
                r = requests.post(f"{api_endpoint}/register", json=stac_catalog, headers=headers)
                r.raise_for_status()
                logger.info(f"Register processing results response: {r.status_code}")
            else:
                logger.info(f"Register processing results to global collection: {collection_id}")
                stac_catalog = {"type": "stac-item", "url": output['StacCatalogUri'].replace("/catalog.json", "")}
                logger.info(f"stac_catalog (GLOBAL)= {stac_catalog}")

                # get collection
                catalog_response = requests.get(f"{self.resource_catalog_api_url}/collections/{collection_id}")
                catalog_response.raise_for_status()
                logger.info(f"catalog_response = {catalog_response.status_code}")
                
                catalog_response_body = catalog_response.json()
                if catalog_response_body.get("id") is None:
                    # if there is no collection, register one
                    logger.info(f"Registering new collection {collection_id}")
                    new_collection_response = requests.post(f"{self.registration_api_url}/register-collection", json=collection_dict)
                    new_collection_response.raise_for_status()
                    logger.info(f"new_collection_response = {new_collection_response.status_code}")

                # register the processing result
                logger.info(f"Registering processing results to global collection: {collection_id}")
                stac_catalog = {"type": "stac-item", "url": output['StacCatalogUri'].replace("/catalog.json", "")}
                r = requests.post(f"{self.registration_api_url}/register", json=stac_catalog)
                r.raise_for_status()
                logger.info(f"Register processing results response: {r.status_code}")


        except Exception as e:
            logger.error("ERROR in post_execution_hook...")
            logger.error(traceback.format_exc())
            raise(e)
        
        finally:
            self.restore_http_proxy_env()

    def unset_http_proxy_env(self):
        http_proxy = os.environ.pop("HTTP_PROXY", None)
        logger.info(f"Unsetting env HTTP_PROXY, whose value was {http_proxy}")

    def restore_http_proxy_env(self):
        if self.http_proxy_env:
            os.environ["HTTP_PROXY"] = self.http_proxy_env
            logger.info(f"Restoring env HTTP_PROXY, to value {self.http_proxy_env}")

    @staticmethod
    def init_config_defaults(conf):
        if "additional_parameters" not in conf:
            conf["additional_parameters"] = {}

        conf["additional_parameters"]["STAGEIN_AWS_SERVICEURL"] = os.environ.get("STAGEIN_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEIN_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEIN_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEIN_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEIN_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEIN_AWS_REGION"] = os.environ.get("STAGEIN_AWS_REGION", "RegionOne")

        conf["additional_parameters"]["STAGEOUT_AWS_SERVICEURL"] = os.environ.get("STAGEOUT_AWS_SERVICEURL", "http://s3-service.zoo.svc.cluster.local:9000")
        conf["additional_parameters"]["STAGEOUT_AWS_ACCESS_KEY_ID"] = os.environ.get("STAGEOUT_AWS_ACCESS_KEY_ID", "minio-admin")
        conf["additional_parameters"]["STAGEOUT_AWS_SECRET_ACCESS_KEY"] = os.environ.get("STAGEOUT_AWS_SECRET_ACCESS_KEY", "minio-secret-password")
        conf["additional_parameters"]["STAGEOUT_AWS_REGION"] = os.environ.get("STAGEOUT_AWS_REGION", "RegionOne")
        conf["additional_parameters"]["STAGEOUT_OUTPUT"] = os.environ.get("STAGEOUT_OUTPUT", "eoepca")

        # DEBUG
        logger.info(f"init_config_defaults: additional_parameters...\n{json.dumps(conf['additional_parameters'], indent=2)}\n")

    @staticmethod
    def get_user_name(decodedJwt) -> str:
        logger.info(f"decodedJwt = {decodedJwt}")
        for key in ["username", "user_name", "preferred_username"]:
            if key in decodedJwt:
                return decodedJwt[key]
        return ""

    @staticmethod
    def local_get_file(fileName):
        """
        Read and load the contents of a yaml file

        :param yaml file to load
        """
        try:
            with open(fileName, "r") as file:
                data = yaml.safe_load(file)
            return data
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}

    def get_pod_env_vars(self):
        logger.info("get_pod_env_vars")

        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        logger.info("get_pod_node_selector")

        return self.conf.get("pod_node_selector", {})

    def get_secrets(self):
        logger.info("get_secrets")

        # It's getting assets from a local file! This folder is shared among all workspaces.
        # TODO: Store image secret on K8s (inside workspace)
        return self.local_get_file("/assets/pod_imagePullSecrets.yaml")

    def get_additional_parameters(self):
        logger.info("get_additional_parameters")

        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs, namespace):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
        try:
            logger.info("Starting handle_outputs")

            logger.info(f"tool_logs = {tool_logs}")
            logger.info(f"output = {output}")
            # logger.info(f"log = {log}")
            logger.info(f"usage_report = {usage_report}")

            # save log to file
            log_name = "logs.log"
            log_file = os.path.join(self.conf["main"]["tmpPath"], namespace, log_name)
            logger.info(f"saving log to file {log_file}")
            with open(log_file, "w") as f:
                f.write(log)

            tool_logs.append(f"./{log_name}")

            # link element to add to the statusInfo
            servicesLogs = [
                {
                    "url": os.path.join(self.conf['main']['tmpUrl'],
                                        f"{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}",
                                        os.path.basename(tool_log)),
                    "title": f"Tool log {os.path.basename(tool_log)}",
                    "rel": "related",
                }
                for tool_log in tool_logs
            ]

            logger.info(f"servicesLogs constructed = {servicesLogs}")

            if "service_logs" not in self.conf:
                self.conf["service_logs"] = {}

            for i in range(len(servicesLogs)):
                okeys = ["url", "title", "rel"]
                keys = ["url", "title", "rel"]
                if i > 0:
                    for j in range(len(keys)):
                        keys[j] = keys[j] + "_" + str(i)
                for j in range(len(keys)):
                    self.conf["service_logs"][keys[j]] = servicesLogs[i][okeys[j]]

            self.conf["service_logs"]["length"] = str(len(servicesLogs))

            logger.info(f"servicesLogs = {json.dumps(self.conf['service_logs'], indent=4)}")

        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise(e)


@dataclass
class StorageCredentials:
    access: str
    bucketname: str
    projectid: str
    secret: str
    endpoint: str
    region: str

@dataclass
class Endpoint:
    id: str
    url: str

@dataclass
class ContainerRegistry:
    username: str
    password: str

@dataclass
class WorkspaceCredentials:
    status: str
    endpoints: list[Endpoint]
    storage: StorageCredentials
    container_registry: ContainerRegistry


class JobInformation:
    def __init__(self, conf: dict[str, Any]):
        self.conf = conf
        self.tmp_path = conf["main"]["tmpPath"]
        self.process_identifier = conf["lenv"]["Identifier"]
        self.process_usid = conf["lenv"]["usid"]
        self.namespace = conf.get("zooServicesNamespace", {}).get("namespace", "")
        self.workspace_prefix = conf.get("eoepca", {}).get("workspace_prefix", "ws")
        self.input_parameters = self._parse_input_parameters()

    @property
    def workspace(self):
        return f"{self.workspace_prefix}-{self.namespace}"
    
    @property
    def working_dir(self):
        return os.path.join(self.tmp_path, f"{self.process_identifier}-{self.process_usid}")

    def _parse_input_parameters(self):
        """
        Parse the input parameters from the request

        :param input_parameters: The input parameters from the request
        """
        json_request = self.conf.get("request", {}).get("jrequest", {})
        json_request = json.loads(json_request)
        logger.info(f"json_request from request: {json_request}")
        
        input_parameters = {}
        for key, value in json_request.get("inputs", {}).items():
            logger.info(f"key = {key}, value = {value}")
            if isinstance(value, dict) or isinstance(value, list):
                input_parameters[key] = json.dumps(value)
            else:
                input_parameters[key] = value

        return [{ 'name': k, 'value': v } for k, v in input_parameters.items()]
    
    def __repr__(self) -> str:
        return f"""
        ************** Job Information **********************
        tmp_path = {self.tmp_path}
        process_identifier = {self.process_identifier}
        process_usid = {self.process_usid}
        workspace = {self.workspace}
        working_dir = {self.working_dir}
        input_parameters = {json.dumps(self.input_parameters, indent=2)}
        *****************************************************
        """
    
    


def get_credentials(workspace: str) -> WorkspaceCredentials:
    os.environ.pop("HTTP_PROXY", None)
    logger.info("Getting credentials")
    response = requests.get(f"http://workspace-api.rm:8080/workspaces/{workspace}")
    response.raise_for_status()

    response_api = response.json()

    endpoints = [Endpoint(id=e['id'], url=e['url']) for e in response_api['endpoints']]
    storage = StorageCredentials(
        access=response_api['storage']['credentials']['access'],
        bucketname=response_api['storage']['credentials']['bucketname'],
        projectid=response_api['storage']['credentials']['projectid'],
        secret=response_api['storage']['credentials']['secret'],
        endpoint=response_api['storage']['credentials']['endpoint'],
        region=response_api['storage']['credentials']['region']
    )
    container_registry = ContainerRegistry(
        username=response_api['container_registry']['username'],
        password=response_api['container_registry']['password']
    )

    return WorkspaceCredentials(
        status=response_api['status'],
        endpoints=endpoints,
        storage=storage,
        container_registry=container_registry
    )

def load_workflow_template_from_file():
    logger.info("open file: app-package.cwl")
    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        argo_template = yaml.safe_load(stream)
    
    return argo_template

def register_catalog(job_information: JobInformation):
    os.environ.pop("HTTP_PROXY", None)
    workspace_api_endpoint = "http://workspace-api.rm:8080"
    stac_catalog = {"type": "stac-item", "url": f"s3://{job_information.workspace}/processing-results/{job_information.process_usid}"}
    logger.info(f"registering stac_catalog = {stac_catalog}")
    headers = {
        "Content-Type": "application/json",
    }
    r = requests.post(f"{workspace_api_endpoint}/workspaces/{job_information.workspace}/register", json=stac_catalog, headers=headers)
    r.raise_for_status()
    logger.info(f"Register processing results response: {r.status_code}")


def prepare_work_directory(job_information: JobInformation):
    os.makedirs(
        job_information.working_dir,
        mode=0o777,
        exist_ok=True,
    )
    os.chdir(job_information.working_dir)

def execute_runner(conf, inputs, outputs):
    try:
        logger.info(f"conf = {json.dumps(conf, indent=4)}")
        logger.info(f"inputs = {json.dumps(inputs, indent=4)}")
        logger.info(f"outputs = {json.dumps(outputs, indent=4)}")

        argo_template = load_workflow_template_from_file()

        job_information = JobInformation(conf)
        logger.info(job_information)

        prepare_work_directory(job_information)

        logger.info("Starting execute runner")

        # run workflow on Argo
        # from API
        logger.info(f"preparing job on workspace {job_information.workspace} with process (workflow) {job_information.process_usid}")

        # get Storage credentials from workspace-api. 
        # TODO: Use the default storage credentials for the global workspace
        workspace_credentials = get_credentials(job_information.workspace)

        #############################################################
        workflow_config = WorkflowConfig(
            namespace=job_information.workspace,
            workflow_id=job_information.process_usid,
            workflow_parameters=job_information.input_parameters,
            storage_credentials=WorkflowStorageCredentials(
                url=workspace_credentials.storage.endpoint,
                access_key=workspace_credentials.storage.access,
                secret_key=workspace_credentials.storage.secret,
            ),
        )

        # run the workflow
        logger.info("Running workflow")
        argo_workflow = ArgoWorkflow(workflow_config=workflow_config, conf=conf)
        exit_status = argo_workflow.run_workflow_from_file(argo_template)

        # handle the outputs
        argo_workflow.save_workflow_logs()

        # if there is a collection_id on the input, add the processed item into that collection
        if exit_status == zoo.SERVICE_SUCCEEDED:
            # Register Catalog
            # TODO: consider more use cases
            logger.info("Registering catalog")
            register_catalog(job_information)
            
            logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            outputs[list(outputs.keys())[0]]["value"] = execution_handler.feature_collection
            logger.info(f"outputs = {json.dumps(outputs, indent=4)}")

        else:
            error_message = zoo._("Execution failed")
            logger.error(f"Execution failed: {error_message}")
            conf["lenv"]["message"] = error_message
            exit_status = zoo.SERVICE_FAILED
        
        # Clean up the namespace
        # argo_workflow.delete_workflow()

        return exit_status

    except Exception as e:
        logger.error("ERROR in processing execution template...")
        stack = traceback.format_exc()
        logger.error(stack)
        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")
        return zoo.SERVICE_FAILED


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa
    return execute_runner(conf, inputs, outputs)
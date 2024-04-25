# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
import pathlib
import sys
print(f"python path = {sys.path}")
print('getcwd:      ', os.getcwd())
print('__file__:    ', __file__)

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
import platform
from urllib.parse import urlparse

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
    def __init__(self, conf):
        super().__init__()
        logger.info("Init EoepcaCalrissianRunnerExecutionHandler")
        
        self.conf = conf

        # #####################################################################################################
        # TODO: check why we need this
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
            # ###############################################
            # TODO: check where this is used
            # ###############################################
            self.conf["additional_parameters"]["collection_id"] = lenv.get("usid", "")
            self.conf["additional_parameters"]["process"] = os.path.join("processing-results", self.conf["additional_parameters"]["collection_id"])
            logger.info(f"conf.additional_parameters.collection_id = {self.conf['additional_parameters']['collection_id']}")
            logger.info(f"conf.additional_parameters.process = {self.conf['additional_parameters']['process']}")

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
            
                logger.info(f"Register processing results to collection")
                stac_catalog = {"type": "stac-item", "url": output['StacCatalogUri'].replace("/catalog.json", "")}
                logger.info(f"stac_catalog = {stac_catalog}")
                r = requests.post(f"{api_endpoint}/register",
                                json=stac_catalog,
                                headers=headers,)
                r.raise_for_status()
                logger.info(f"Register processing results response: {r.status_code}")
            else:
                logger.info("Registering processing result into Global (NOT IMPLEMENTED)")
                # #####################################################################################################
                # TODO: fill this part
                # #####################################################################################################

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

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The metrics file.
        :param tool_logs: A list of paths to individual workflow step logs.

        """
        try:
            logger.info("Starting handle_outputs")

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
            for i in range(len(servicesLogs)):
                okeys = ["url", "title", "rel"]
                keys = ["url", "title", "rel"]
                if i > 0:
                    for j in range(len(keys)):
                        keys[j] = keys[j] + "_" + str(i)
                if "service_logs" not in self.conf:
                    self.conf["service_logs"] = {}
                for j in range(len(keys)):
                    self.conf["service_logs"][keys[j]] = servicesLogs[i][okeys[j]]

            self.conf["service_logs"]["length"] = str(len(servicesLogs))

            logger.info(f"servicesLogs = {json.dumps(self.conf['service_logs'], indent=4)}")

        except Exception as e:
            logger.error("ERROR in handle_outputs...")
            logger.error(traceback.format_exc())
            raise(e)


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs): # noqa
    try:
        logger.info(f"Starting service.py on node {platform.node()}")
        logger.info(f"inputs = {json.dumps(inputs, indent=4)}")
        logger.info(f"outputs = {json.dumps(outputs, indent=4)}")

        logger.info("open file: app-package.cwl")
        with open(
            os.path.join(
                pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
                "app-package.cwl",
            ),
            "r",
        ) as stream:
            cwl = yaml.safe_load(stream)

        logger.info(f"Creating custom execution_handler with conf")
        execution_handler = EoepcaCalrissianRunnerExecutionHandler(conf=conf)

        logger.info(f"conf (with modification done by EoepcaCalrissianRunnerExecutionHandler):\n{json.dumps(conf, indent=4)}")

        logger.info("Creating ZooCalrissianRunner runner")
        # #####################################################################################################
        # TODO: image_pull_secrets are handle by get_secrets method on the EoepcaCalrissianRunnerExecutionHandler
        # #####################################################################################################
        runner = ZooCalrissianRunner(
            cwl=cwl,
            conf=conf,
            inputs=inputs,
            outputs=outputs,
            execution_handler=execution_handler,
        )
        # DEBUG
        # runner.monitor_interval = 1

        # we are changing the working directory to store the outputs
        # in a directory dedicated to this execution
        working_dir = os.path.join(conf["main"]["tmpPath"], runner.get_namespace_name())
        logger.info(f"Working directory: {working_dir}")
        os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
        )
        os.chdir(working_dir)

        logger.info("Starting execute runner")

        # This is a blocking execution
        exit_status = runner.execute()

        if exit_status == zoo.SERVICE_SUCCEEDED:
            logger.info(f"Setting Collection into output key {list(outputs.keys())[0]}")
            outputs[list(outputs.keys())[0]]["value"] = execution_handler.feature_collection
            return zoo.SERVICE_SUCCEEDED

        else:
            error_message = zoo._("Execution failed")
            logger.error(f"Execution failed: {error_message}")
            conf["lenv"]["message"] = error_message
            return zoo.SERVICE_FAILED

    except Exception as e:
        logger.error("ERROR in processing execution template...")
        stack = traceback.format_exc()
        logger.error(stack)
        conf["lenv"]["message"] = zoo._(f"Exception during execution...\n{stack}\n")
        return zoo.SERVICE_FAILED

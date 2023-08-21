import json
from copy import deepcopy
from shlex import split as shsplit
from time import sleep
import logging
from pprint import pformat


from kubernetes import config, client, watch, stream

from kubernetes.client.api_client import ApiClient
from kubernetes.client.models import V1Pod, V1PodList, V1Deployment, V1DeploymentList, V1Service, V1ServiceList
from jsonmerge import merge

config.load_kube_config()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(name)s %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BaseObject(object):
    openapi_model = "str"
    _default_manifest = {}
    _base_client = ApiClient()

    def __init__(self):
        self.manifest: dict = {}
        self.model = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def merge_partial_manifest(self, partial_manifest):
        self.manifest = merge(self.manifest, partial_manifest)

    def deserialize(self):
        class Temp:
            data = None
        t = Temp()
        t.data = json.dumps(self.manifest)
        return self._base_client.deserialize(t, self.openapi_model)

    def serialize(self):
        return self._base_client.sanitize_for_serialization(self.model)

    def to_pretty(self):
        return json.dumps(self.serialize(), indent=4)

    def __repr__(self):
        return json.dumps(self.serialize())


class Pod(BaseObject):
    openapi_model = "V1Pod"
    _default_manifest = {}
    _cls_logger = logging.getLogger()

    def __init__(self, body=None, partial_manifest=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client.CoreV1Api()
        self._watch = watch.Watch()

        if body is None:
            self.manifest = self.default_manifest()
        else:
            self.manifest = body

        if partial_manifest:
            self.merge_partial_manifest(partial_manifest)

        if not self.manifest.get("metadata", {}).get("name") or not self.manifest.get("metadata", {}).get("namespace"):
            raise Exception("name & namespace both not specified while initializing pod")

        self.model: V1Pod = self.deserialize()

    @property
    def name(self):
        return self.model.metadata.name

    @property
    def namespace(self):
        return self.model.metadata.namespace

    @property
    def namespaced_name(self):
        return f"{self.namespace}/{self.name}"

    @property
    def uid(self):
        return self.model.metadata.uid

    @classmethod
    def get(cls, namespace, name):
        api_client = client.CoreV1Api()
        resp = api_client.read_namespaced_pod(name=name, namespace=namespace)
        return cls(body=cls._base_client.sanitize_for_serialization(resp))

    @classmethod
    def list(cls, namespace, **kwargs):
        api_client = client.CoreV1Api()
        resp: V1PodList = api_client.list_namespaced_pod(namespace=namespace, **kwargs)
        pod_list = []
        for item in resp.items:
            pod_list.append(cls(body=cls._base_client.sanitize_for_serialization(item)))
        return pod_list

    def default_manifest(self):
        return self._default_manifest

    def set_default_manifest(self, manifest):
        self._default_manifest = manifest

    def create(self, wait_for_completion=True, timeout=60, **kwargs):
        self._logger.info(f"Creating pod {self.namespaced_name}")
        self._logger.info(self.to_pretty())
        self.model = self._client.create_namespaced_pod(namespace=self.namespace, body=self.model, **kwargs)

        if wait_for_completion:
            for event in self._watch.stream(func=self._client.list_namespaced_pod,
                                            namespace=self.model.metadata.namespace,
                                            timeout_seconds=timeout, **kwargs):
                if event["object"].status.phase == 'Running' and event["object"].metadata.name == self.name:
                    self._watch.stop()
                    self._logger.info(f"Pod created {self.namespaced_name}")
                    return True
                if event["type"] == "DELETED" and event["object"].metadata.name == self.name:
                    self._watch.stop()
                    self._logger.warning(f"Pod deleted {self.namespaced_name}")
                    return False
            self._logger.warning(f"Unable to get created pod {self.namespaced_name}")
            return False
        self._logger.info(f"Pod created {self.namespaced_name}")
        return True

    def read(self, **kwargs):
        self.model = self._client.read_namespaced_pod(self.name, self.namespace, **kwargs)

    def delete(self, wait_for_completion=True, timeout=60, **kwargs):
        self._logger.info(f"Deleting pod {self.namespaced_name}")
        self._client.delete_namespaced_pod(self.name, self.namespace, **kwargs)
        if wait_for_completion:
            for event in self._watch.stream(func=self._client.list_namespaced_pod,
                                            namespace=self.namespace,
                                            timeout_seconds=timeout, **kwargs):
                if event["type"] == "DELETED" and event["object"].metadata.uid == self.uid:
                    self._watch.stop()
                    self._logger.info(f"Deleted pod {self.namespaced_name}")
                    self.model = client.V1Pod()
                    return True
            self._logger.warning(f"Unable to delete pod {self.namespaced_name}")
            return False
        self.model = client.V1Pod()
        self._logger.info(f"Deleted pod {self.namespaced_name}")
        return True

    def update(self, body, **kwargs):
        self.model = self._client.patch_namespaced_pod(self.name, self.namespace, body=body, **kwargs)

    def exec(self, container, command):
        return stream.stream(self._client.connect_get_namespaced_pod_exec,
                             self.model.metadata.name,
                             self.model.metadata.namespace,
                             container=container,
                             command=shsplit(command),
                             stderr=True, stdin=False,
                             stdout=True, tty=False)

    def logs(self, container):
        return self._client.read_namespaced_pod_log(self.model.metadata.name,
                                                    self.model.metadata.namespace,
                                                    container=container)

    def __enter__(self):
        if self.create(wait_for_completion=True):
            return self
        else:
            print("unable to get pod ready")
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.delete(wait_for_completion=True):
            print("unable to delete pod")


class Deployment(BaseObject):
    openapi_model = "V1Deployment"
    _default_manifest = {}

    def __init__(self, body=None, partial_manifest=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client.AppsV1Api()
        self._watch = watch.Watch()

        if body is None:
            self.manifest = self.default_manifest()
        else:
            self.manifest = body

        if partial_manifest:
            self.merge_partial_manifest(partial_manifest)

        if not self.manifest.get("metadata", {}).get("name") or not self.manifest.get("metadata", {}).get("namespace"):
            raise Exception("name & namespace both not specified while initializing deployment")

        self.model: V1Deployment = self.deserialize()

    @property
    def name(self):
        return self.model.metadata.name

    @property
    def namespace(self):
        return self.model.metadata.namespace

    @property
    def namespaced_name(self):
        return f"{self.namespace}/{self.name}"

    @property
    def uid(self):
        return self.model.metadata.uid

    @classmethod
    def get(cls, namespace, name):
        api_client = client.AppsV1Api()
        resp = api_client.read_namespaced_deployment(name=name, namespace=namespace)
        return cls(body=cls._base_client.sanitize_for_serialization(resp))

    @classmethod
    def list(cls, namespace, **kwargs):
        api_client = client.AppsV1Api()
        resp: V1DeploymentList = api_client.list_namespaced_deployment(namespace=namespace, **kwargs)
        _list = []
        for item in resp.items:
            _list.append(cls(body=cls._base_client.sanitize_for_serialization(item)))
        return _list

    def default_manifest(self):
        return self._default_manifest

    def set_default_manifest(self, manifest):
        self._default_manifest = manifest

    def create(self, wait_for_completion=True, timeout=60, **kwargs):
        self._logger.info(self.to_pretty())
        self.model = self._client.create_namespaced_deployment(namespace=self.namespace, body=self.model, **kwargs)
        if wait_for_completion:
            for event in self._watch.stream(func=self._client.list_namespaced_deployment,
                                            namespace=self.model.metadata.namespace,
                                            timeout_seconds=timeout, **kwargs):
                if event["object"].status.available_replicas == self.model.spec.replicas and \
                        event["object"].metadata.name == self.name:
                    self._watch.stop()
                    return True
                if event["type"] == "DELETED" and event["object"].metadata.name == self.name:
                    self._watch.stop()
                    return False
            print(f"unable to get created deployment {self.namespaced_name}")
            return False
        return True

    def read(self, **kwargs):
        self.model = self._client.read_namespaced_deployment(self.name, self.namespace, **kwargs)

    def delete(self, wait_for_completion=True, timeout=30, **kwargs):
        if wait_for_completion:
            watcher = self._watch.stream(func=self._client.list_namespaced_deployment,
                                                namespace=self.model.metadata.namespace,
                                                timeout_seconds=timeout, **kwargs)
        self._client.delete_namespaced_deployment(self.name, self.namespace, **kwargs)
        if wait_for_completion:
            for event in watcher:
                if event["type"] == "DELETED" and event["object"].metadata.uid == self.uid:
                    self._watch.stop()
                    return True
            print(f"unable to delete deployment {self.namespaced_name}")
            return False
        self.model = client.V1Deployment()
        return True

    def update(self, body, **kwargs):
        self.model = self._client.patch_namespaced_deployment(self.name, self.namespace, body=body, **kwargs)

    def __enter__(self):
        if self.create(wait_for_completion=True):
            return self
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.delete(wait_for_completion=True):
            print("unable to delete deployment")


def main():
    pm = {'apiVersion': 'v1',
          'kind': 'Pod',
          'metadata': {'namespace':'default','name': 'sample-pod2', 'labels': {'app': 'l1'}},
          'spec': {
              'containers': [
                  {'ports':[
                      {'containerPort':80,
                       'protocol':'TCP'}],
                    'image': 'busybox',
                    'name': 'sleep',
                    'args': ['/bin/sh', '-c', 'while true;do date;sleep 5; done']
                  }
                ]
            }
        }

    with Pod(body=pm) as p:
        print(p.exec("sleep", "ip a"))

    dm = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "labels": {
                "app": "nginx"
            },
            "name": "nginx-deployment",
            "namespace": "default"
        },
        "spec": {
            "replicas": 3,
            "selector": {
                "matchLabels": {
                    "app": "nginx"
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "app": "nginx"
                    }
                },
                "spec": {
                    "containers": [
                        {
                            "image": "nginx:1.14.2",
                            "imagePullPolicy": "IfNotPresent",
                            "name": "nginx",
                            "ports": [
                                {
                                    "containerPort": 80,
                                    "protocol": "TCP"
                                }
                            ]
                        }
                    ]
                }
            }
        }
    }

    # with Deployment(body=dm) as d:
    #     print(d.model.spec.replicas)
    #     while d.model.status.available_replicas == d.model.spec.replicas:
    #         print(d.uid)
    #         sleep(1)

if __name__ == '__main__':
    main()
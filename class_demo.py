import time
from typing import Union
import yaml

from kubernetes import config, client, watch, stream

config.load_kube_config()


class PodManager:
    def __init__(self, pod: Union[client.V1Pod, dict]):
        print("init")
        self._client = client.CoreV1Api()
        self._watch = watch.Watch()
        self.pod = pod

    def create(self, namespace: str, **kwargs):
        print("create")
        res = self._client.create_namespaced_pod(namespace=namespace, body=self.pod, **kwargs)
        self.pod = res

    def read(self):
        return self._client.read_namespaced_pod(self.pod.metadata.name, self.pod.metadata.namespace)

    def get(self, namespace, **kwargs):
        return self._client.list_namespaced_pod(namespace, **kwargs)

    def delete(self, name: str, namespace: str, **kwargs):
        _ = self._client.delete_namespaced_pod(name, namespace, **kwargs)
        self.pod = client.V1Pod()
        return True

    def update(self, name: str, namespace: str, **kwargs):
        return self._client.patch_namespaced_pod(name, namespace, **kwargs)

    def ready(self, timeout=60, **kwargs):
        print("ready")
        for event in self._watch.stream(func=self._client.list_namespaced_pod,
                                        namespace=self.pod.metadata.namespace,
                                        timeout_seconds=timeout, **kwargs):
            if event["object"].status.phase == 'Running' and event["object"].metadata.name == self.pod.metadata.name:
                print(event["object"].metadata.name)
                self._watch.stop()
                return True
            if event["type"] == "DELETED":
                self._watch.stop()
                return False

    def exec(self, command):
        resp = stream.stream(self._client.connect_get_namespaced_pod_exec,
                             self.pod.metadata.name,
                             self.pod.metadata.namespace,
                             command=command,
                             stderr=True, stdin=False,
                             stdout=True, tty=False)
        return resp

    def logs(self):
        return self._client.read_namespaced_pod_log(self.pod.metadata.name, self.pod.metadata.namespace)

    def __enter__(self):
        print("__enter__")
        self.create('default')
        if self.ready():
            return self
        else:
            print("unable to get pod ready")
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.delete(self.pod.metadata.name, self.pod.metadata.namespace)


class DeploymentManager:
    def __init__(self, deployment: Union[client.V1Deployment, dict]):
        self._client = client.AppsV1Api()
        self._watch = watch.Watch()
        self.deployment = deployment

    def create(self, namespace, **kwargs):
        print("create")
        res = self._client.create_namespaced_deployment(namespace, body=self.deployment, **kwargs)
        self.deployment = res

    def read(self):
        return self._client.read_namespaced_deployment(self.deployment.metadata.name, self.deployment.metadata.namespace)

    def delete(self):
        return self._client.delete_namespaced_deployment(self.deployment.metadata.name, self.deployment.metadata.namespace)

    def ready(self, timeout=60, **kwargs):
        print("ready")
        for event in self._watch.stream(func=self._client.list_namespaced_deployment,
                                        namespace=self.deployment.metadata.namespace,
                                        timeout_seconds=timeout, **kwargs):
            print(event["type"])
            if event["object"].metadata.name == self.deployment.metadata.name and \
                    event["object"].status.available_replicas == self.deployment.spec.replicas:
                print(event["object"].metadata.name)
                self._watch.stop()
                return True
            if event["type"] == "DELETED":
                self._watch.stop()
                return False


if __name__ == '__main__':
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': "sample-pod",
            'labels': {
                "app": "l1"
            }
        },
        'spec': {
            'containers': [{
                'image': 'busybox',
                'name': 'sleep',
                "args": [
                    "/bin/sh",
                    "-c",
                    "while true;do date;sleep 5; done"
                ]
            }]
        }
    }
    # podm = PodManager(pod=pod_manifest)
    # print(type(podm.create('default')))

    # print(type(podm.get('default')))
    # print(podm.ready())

    """
    with PodManager(pod=pod_manifest) as podm:
        print("hello")
        print(podm.read().metadata.labels)
        print(podm.exec(["/bin/sh", "-c", "ls -l"]))
        print(podm.logs())
        print("Sleeping 10 seconds")
        time.sleep(10)
        print("exiting")
    """
    with open("deploy.yaml") as f:
        dep = yaml.safe_load(f)

    dm = DeploymentManager(dep)
    dm.create('default')
    dm.ready()
    print(dm.read())
    time.sleep(5)
    dm.delete()





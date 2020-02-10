import uuid

import pykube
import pytest
from mock import MagicMock

from zmon_worker_monitor.builtins.plugins.kubernetes import (
    KubernetesWrapper,
    CheckError,
    _get_resources,
)

CLUSTER_URL = "https://kube-cluster.example.org"


def resource_mock(name, phase=None, replicas=None, ready=None):
    resource = MagicMock(name="pykube-{}".format(name))
    resource.name = name
    resource.obj = {"metadata": {"name": name, "uid": uuid.uuid4()}}
    if phase is not None:
        resource.obj["status"] = {"phase": phase}
    if replicas is not None:
        resource.replicas = replicas
    if ready is not None:
        resource.ready = ready
    return resource


class MockWrapper:
    def __init__(self, monkeypatch, kind, namespace, resources, owning_module="pykube"):
        self._query = MagicMock(name="query")
        self._object_manager = MagicMock(name="object_manager")
        self._object_manager.objects.return_value = self._query

        self._get_resources = MagicMock("get_resources")
        self._get_resources.return_value = resources

        monkeypatch.setattr("{}.{}".format(owning_module, kind), self._object_manager)
        monkeypatch.setattr(
            "zmon_worker_monitor.builtins.plugins.kubernetes._get_resources",
            self._get_resources,
        )

        self._client = client_mock(monkeypatch)
        self.wrapper = KubernetesWrapper(namespace=namespace, check_id='<test>')

    def assert_objects_called(self, expected_namespace=None):
        if expected_namespace is not None:
            self._object_manager.objects.assert_called_once_with(
                self._client, expected_namespace
            )
        else:
            self._object_manager.objects.assert_called_once_with(self._client)

    def assert_get_resources_called(self, expected_args):
        self._get_resources.assert_called_once_with(self._query, **expected_args)


def client_mock(monkeypatch):
    monkeypatch.setattr("pykube.KubeConfig", MagicMock())
    client = MagicMock(name="client")
    client.config.cluster = {'server': CLUSTER_URL}
    monkeypatch.setattr("pykube.HTTPClient", lambda *args, **kwargs: client)
    return client


def test_get_resources_named():
    resource = MagicMock(name="resource")

    manager = MagicMock()
    manager.namespace = "default"
    manager.get_by_name.return_value = resource

    assert [resource] == _get_resources(manager, name="foo")
    manager.get_by_name.assert_called_once_with("foo")


def test_get_resources_named_no_resource():
    manager = MagicMock()
    manager.namespace = "default"
    manager.get_by_name.side_effect = pykube.exceptions.ObjectDoesNotExist()

    assert [] == _get_resources(manager, name="foo")
    manager.get_by_name.assert_called_once_with("foo")


@pytest.mark.parametrize(
    "namespace,phase,kwargs",
    [
        (pykube.all, None, {}),
        ("default", "Pending", {}),
        ("default", None, {"application": "foo"}),
    ],
)
def test_get_resources_unsupported(namespace, phase, kwargs):
    manager = MagicMock()
    manager.namespace = namespace
    manager.get_by_name.return_value = None

    with pytest.raises(CheckError):
        _get_resources(manager, name="foo", phase=phase, **kwargs)

    manager.get_by_name.assert_not_called()


@pytest.mark.parametrize(
    "field_selector,kwargs,query",
    [
        (None, {}, {}),
        (
            {"status.phase": "Pending"},
            {},
            {"field_selector": {"status.phase": "Pending"}},
        ),
        (None, {"application": "foo"}, {"selector": {"application": "foo"}}),
        (
            {"status.phase": "Pending"},
            {"application": "foo"},
            {
                "field_selector": {"status.phase": "Pending"},
                "selector": {"application": "foo"},
            },
        ),
    ],
)
def test_get_resources_filter(field_selector, kwargs, query):
    manager = MagicMock()
    manager.filter.return_value = [1, 2, 3]

    assert [1, 2, 3] == _get_resources(
        manager, name=None, field_selector=field_selector, **kwargs
    )
    manager.filter.assert_called_once_with(**query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,phase,ready,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            pykube.all,
            None,
            None,
            "foo",
            {},
            {"name": "foo", "field_selector": None},
            [resource_mock(name="pod-1", phase="Pending", ready=True)],
            {"pod-1"},
        ),
        (
            "default",
            "default",
            None,
            None,
            "foo",
            {},
            {"name": "foo", "field_selector": None},
            [resource_mock(name="pod-1", phase="Pending", ready=True)],
            {"pod-1"},
        ),
        (
            None,
            pykube.all,
            None,
            None,
            None,
            {"application": "foo"},
            {"name": None, "field_selector": None, "application": "foo"},
            [resource_mock(name="pod-1", phase="Pending", ready=True)],
            {"pod-1"},
        ),
        (
            "foobar",
            "foobar",
            None,
            None,
            None,
            {"application": "foo"},
            {"name": None, "field_selector": None, "application": "foo"},
            [
                resource_mock(name="pod-1", phase="Pending", ready=True),
                resource_mock(name="pod-2", phase="Pending", ready=True),
            ],
            {"pod-1", "pod-2"},
        ),
        (
            "foobar",
            "foobar",
            "Running",
            False,
            None,
            {"application": "foo"},
            {
                "name": None,
                "field_selector": {"status.phase": "Running"},
                "application": "foo",
            },
            [
                resource_mock(name="pod-2", phase="Running", ready=False),
                resource_mock(name="pod-3", phase="Running", ready=True),
            ],
            {"pod-2"},
        ),
    ],
)
def test_pods(
    monkeypatch,
    namespace,
    expected_namespace,
    phase,
    ready,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "Pod", namespace, objects)
    pods = mock.wrapper.pods(name=name, phase=phase, ready=ready, **kwargs)
    assert [r.obj for r in objects if r.name in expected_objects] == pods
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize("kwargs", ({"ready": 1}, {"ready": 0}, {"phase": "WRONG"}))
def test_pods_error(kwargs):
    k = KubernetesWrapper(check_id='<test>')

    with pytest.raises(CheckError):
        k.pods(**kwargs)


def test_namespaces(monkeypatch):
    client_mock(monkeypatch)

    res = [resource_mock({"metadata": {}})]

    ns = MagicMock()
    ns.objects.return_value.all.return_value = res
    monkeypatch.setattr("pykube.Namespace", ns)

    k = KubernetesWrapper(check_id='<test>')
    namespaces = k.namespaces()

    assert [r.obj for r in res] == namespaces


@pytest.mark.parametrize(
    "namespace,name,kwargs,expected_query,objects",
    [
        (None, "foo", {}, {"name": "foo"}, [resource_mock(name="node-1")]),
        ("default", "foo", {}, {"name": "foo"}, [resource_mock(name="node-1")]),
        (
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="node-1")],
        ),
        (
            "default",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="node-1"), resource_mock(name="node-2")],
        ),
    ],
)
def test_nodes(monkeypatch, namespace, name, kwargs, expected_query, objects):
    mock = MockWrapper(monkeypatch, "Node", namespace, objects)
    nodes = mock.wrapper.nodes(name=name, **kwargs)
    assert [r.obj for r in objects] == nodes
    mock.assert_objects_called(expected_namespace=None)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="svc-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="svc-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="svc-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="svc-1"), resource_mock(name="svc-2")],
        ),
    ],
)
def test_services(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "Service", namespace, objects)
    services = mock.wrapper.services(name=name, **kwargs)
    assert [r.obj for r in objects] == services
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="ep-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ep-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ep-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ep-1"), resource_mock(name="ep-2")],
        ),
    ],
)
def test_endpoints(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "Endpoint", namespace, objects)
    endpoints = mock.wrapper.endpoints(name=name, **kwargs)
    assert [r.obj for r in objects] == endpoints
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (
            None,
            pykube.all,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ingress-1")],
        ),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ingress-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ingress-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ingress-1"), resource_mock(name="ingress-2")],
        ),
    ],
)
def test_ingresses(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "Ingress", namespace, objects)
    ingresses = mock.wrapper.ingresses(name=name, **kwargs)
    assert [r.obj for r in objects] == ingresses
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,replicas,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            pykube.all,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ss-1", replicas=1)],
            {"ss-1"},
        ),
        (
            "default",
            "default",
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ss-1", replicas=1)],
            {"ss-1"},
        ),
        (
            None,
            pykube.all,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ss-1", replicas=1)],
            {"ss-1"},
        ),
        (
            "foobar",
            "foobar",
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="ss-1", replicas=1),
                resource_mock(name="ss-2", replicas=1),
            ],
            {"ss-1", "ss-2"},
        ),
        (
            "foobar",
            "foobar",
            2,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="ss-1", replicas=1),
                resource_mock(name="ss-2", replicas=2),
                resource_mock(name="ss-3", replicas=2),
            ],
            {"ss-2", "ss-3"},
        ),
    ],
)
def test_statefulsets(
    monkeypatch,
    namespace,
    expected_namespace,
    replicas,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "StatefulSet", namespace, objects)
    statefulsets = mock.wrapper.statefulsets(name=name, replicas=replicas, **kwargs)
    assert [r.obj for r in objects if r.name in expected_objects] == statefulsets
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="ds-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="ds-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ds-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="ds-1"), resource_mock(name="ds-2")],
        ),
    ],
)
def test_daemonsets(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "DaemonSet", namespace, objects)
    daemonsets = mock.wrapper.daemonsets(name=name, **kwargs)
    assert [r.obj for r in objects] == daemonsets
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,replicas,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            pykube.all,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="rs-1", replicas=1)],
            {"rs-1"},
        ),
        (
            "default",
            "default",
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="rs-1", replicas=1)],
            {"rs-1"},
        ),
        (
            None,
            pykube.all,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="rs-1", replicas=1)],
            {"rs-1"},
        ),
        (
            "foobar",
            "foobar",
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="rs-1", replicas=1),
                resource_mock(name="rs-2", replicas=1),
            ],
            {"rs-1", "rs-2"},
        ),
        (
            "foobar",
            "foobar",
            2,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="rs-1", replicas=1),
                resource_mock(name="rs-2", replicas=2),
                resource_mock(name="rs-3", replicas=2),
            ],
            {"rs-2", "rs-3"},
        ),
    ],
)
def test_replicasets(
    monkeypatch,
    namespace,
    expected_namespace,
    replicas,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "ReplicaSet", namespace, objects)
    replicasets = mock.wrapper.replicasets(name=name, replicas=replicas, **kwargs)
    assert [r.obj for r in objects if r.name in expected_objects] == replicasets
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,replicas,ready,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            pykube.all,
            None,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="dep-1", replicas=1, ready=True)],
            {"dep-1"},
        ),
        (
            "default",
            "default",
            None,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="dep-1", replicas=1, ready=True)],
            {"dep-1"},
        ),
        (
            None,
            pykube.all,
            None,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="dep-1", replicas=1, ready=True)],
            {"dep-1"},
        ),
        (
            "foobar",
            "foobar",
            None,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="dep-1", replicas=1, ready=True),
                resource_mock(name="dep-2", replicas=1, ready=True),
            ],
            {"dep-1", "dep-2"},
        ),
        (
            "foobar",
            "foobar",
            2,
            False,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="dep-1", replicas=1, ready=True),
                resource_mock(name="dep-2", replicas=2, ready=False),
                resource_mock(name="dep-3", replicas=2, ready=True),
                resource_mock(name="dep-4", replicas=3, ready=False),
            ],
            {"dep-2"},
        ),
    ],
)
def test_deployments(
    monkeypatch,
    namespace,
    expected_namespace,
    replicas,
    ready,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "Deployment", namespace, objects)
    deployments = mock.wrapper.deployments(
        name=name, replicas=replicas, ready=ready, **kwargs
    )
    assert [r.obj for r in objects if r.name in expected_objects] == deployments
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize("kwargs", ({"ready": 1}, {"ready": 0}))
def test_deployments_error(kwargs):
    k = KubernetesWrapper(check_id='<test>')

    with pytest.raises(CheckError):
        k.deployments(**kwargs)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="cfg-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="cfg-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="cfg-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="cfg-1"), resource_mock(name="cfg-2")],
        ),
    ],
)
def test_configmaps(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "ConfigMap", namespace, objects)
    configmaps = mock.wrapper.configmaps(name=name, **kwargs)
    assert [r.obj for r in objects] == configmaps
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,phase,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            pykube.all,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="pvc-1", phase="Pending")],
            {"pvc-1"},
        ),
        (
            "default",
            "default",
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="pvc-1", phase="Pending")],
            {"pvc-1"},
        ),
        (
            None,
            pykube.all,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="pvc-1", phase="Pending")],
            {"pvc-1"},
        ),
        (
            "foobar",
            "foobar",
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="pvc-1", phase="Pending"),
                resource_mock(name="pvc-2", phase="Ready"),
            ],
            {"pvc-1", "pvc-2"},
        ),
        (
            "foobar",
            "foobar",
            "Ready",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="pvc-1", phase="Pending"),
                resource_mock(name="pvc-2", phase="Ready"),
                resource_mock(name="pvc-3", phase="Ready"),
            ],
            {"pvc-2", "pvc-3"},
        ),
    ],
)
def test_persistentvolumeclaims(
    monkeypatch,
    namespace,
    expected_namespace,
    phase,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "PersistentVolumeClaim", namespace, objects)
    persistentvolumeclaims = mock.wrapper.persistentvolumeclaims(
        name=name, phase=phase, **kwargs
    )
    assert [
        r.obj for r in objects if r.name in expected_objects
    ] == persistentvolumeclaims
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,phase,name,kwargs,expected_query,objects,expected_objects",
    [
        (
            None,
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="pv-1", phase="Pending")],
            {"pv-1"},
        ),
        (
            "default",
            None,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="pv-1", phase="Pending")],
            {"pv-1"},
        ),
        (
            None,
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="pv-1", phase="Pending")],
            {"pv-1"},
        ),
        (
            "foobar",
            None,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="pv-1", phase="Pending"),
                resource_mock(name="pv-2", phase="Ready"),
            ],
            {"pv-1", "pv-2"},
        ),
        (
            "foobar",
            "Ready",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [
                resource_mock(name="pv-1", phase="Pending"),
                resource_mock(name="pv-2", phase="Ready"),
                resource_mock(name="pv-3", phase="Ready"),
            ],
            {"pv-2", "pv-3"},
        ),
    ],
)
def test_persistentvolumes(
    monkeypatch,
    namespace,
    phase,
    name,
    kwargs,
    expected_query,
    objects,
    expected_objects,
):
    mock = MockWrapper(monkeypatch, "PersistentVolume", namespace, objects)
    persistentvolumes = mock.wrapper.persistentvolumes(name=name, phase=phase, **kwargs)
    assert [r.obj for r in objects if r.name in expected_objects] == persistentvolumes
    mock.assert_objects_called(expected_namespace=None)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="quota-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="quota-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="quota-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="quota-1"), resource_mock(name="quota-2")],
        ),
    ],
)
def test_resourcequotas(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "ResourceQuota", namespace, objects)
    resourcequotas = mock.wrapper.resourcequotas(name=name, **kwargs)
    assert [r.obj for r in objects] == resourcequotas
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="job-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="job-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="job-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="job-1"), resource_mock(name="job-2")],
        ),
    ],
)
def test_jobs(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "Job", namespace, objects)
    jobs = mock.wrapper.jobs(name=name, **kwargs)
    assert [r.obj for r in objects] == jobs
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (
            None,
            pykube.all,
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="cronjob-1")],
        ),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="cronjob-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="cronjob-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="cronjob-1"), resource_mock(name="cronjob-2")],
        ),
    ],
)
def test_cronjobs(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "CronJob", namespace, objects)
    cronjobs = mock.wrapper.cronjobs(name=name, **kwargs)
    assert [r.obj for r in objects] == cronjobs
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="pcs-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="pcs-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="pcs-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="pcs-1"), resource_mock(name="pcs-2")],
        ),
    ],
)
def test_platformcredentialssets(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "PlatformCredentialsSet", namespace, objects,
                       owning_module="zmon_worker_monitor.builtins.plugins.kubernetes")
    configmaps = mock.wrapper.platformcredentialssets(name=name, **kwargs)
    assert [r.obj for r in objects] == configmaps
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="iamrole-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="iamrole-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="iamrole-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="iamrole-1"), resource_mock(name="iamrole-2")],
        ),
    ],
)
def test_awsiamroles(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "AWSIAMRole", namespace, objects,
                       owning_module="zmon_worker_monitor.builtins.plugins.kubernetes")
    configmaps = mock.wrapper.awsiamroles(name=name, **kwargs)
    assert [r.obj for r in objects] == configmaps
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="stackset-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="stackset-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="stackset-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="stackset-1"), resource_mock(name="stackset-2")],
        ),
    ],
)
def test_stacksets(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "StackSet", namespace, objects,
                       owning_module="zmon_worker_monitor.builtins.plugins.kubernetes")
    configmaps = mock.wrapper.stacksets(name=name, **kwargs)
    assert [r.obj for r in objects] == configmaps
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


@pytest.mark.parametrize(
    "namespace,expected_namespace,name,kwargs,expected_query,objects",
    [
        (None, pykube.all, "foo", {}, {"name": "foo"}, [resource_mock(name="stack-1")]),
        (
            "default",
            "default",
            "foo",
            {},
            {"name": "foo"},
            [resource_mock(name="stack-1")],
        ),
        (
            None,
            pykube.all,
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="stack-1")],
        ),
        (
            "foobar",
            "foobar",
            None,
            {"application": "foo"},
            {"name": None, "application": "foo"},
            [resource_mock(name="stack-1"), resource_mock(name="stack-2")],
        ),
    ],
)
def test_stacks(
    monkeypatch, namespace, expected_namespace, name, kwargs, expected_query, objects
):
    mock = MockWrapper(monkeypatch, "Stack", namespace, objects,
                       owning_module="zmon_worker_monitor.builtins.plugins.kubernetes")
    configmaps = mock.wrapper.stacks(name=name, **kwargs)
    assert [r.obj for r in objects] == configmaps
    mock.assert_objects_called(expected_namespace=expected_namespace)
    mock.assert_get_resources_called(expected_query)


def test_metrics(monkeypatch):
    client = client_mock(monkeypatch)

    resp = MagicMock()
    resp.text = "metrics"

    client.session.get.return_value = resp

    parsed = MagicMock()
    parsed.samples = [
        ("metric-1", {}, 20.17),
        ("metric-2", {"verb": "GET"}, 20.16),
        ("metric-1", {"verb": "POST"}, 20.18),
    ]

    parser = MagicMock()
    parser.return_value = [parsed]

    monkeypatch.setattr(
        "zmon_worker_monitor.builtins.plugins.kubernetes.text_string_to_metric_families",
        parser,
    )

    k = KubernetesWrapper(check_id='<test>')
    metrics = k.metrics()

    expected = {
        "metric-1": [({}, 20.17), ({"verb": "POST"}, 20.18)],
        "metric-2": [({"verb": "GET"}, 20.16)],
    }

    assert metrics == expected

    parser.assert_called_with(resp.text)
    client.session.get.assert_called_with(CLUSTER_URL + "/metrics")

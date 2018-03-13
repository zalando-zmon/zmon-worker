import pytest

from mock import MagicMock, call

from zmon_worker_monitor.builtins.plugins.kubernetes import KubernetesWrapper, CheckError

CLUSTER_URL = 'https://kube-cluster.example.org'


def resource_mock(obj, **kwargs):
    resource = MagicMock()
    resource.obj = obj
    resource.ready = True
    for k, v in kwargs.items():
        setattr(resource, k, v)

    return resource


def get_resources_mock(res):
    get = MagicMock()
    get.return_value = res

    return get


def client_mock(monkeypatch):
    monkeypatch.setattr('pykube.KubeConfig', MagicMock())

    client = MagicMock()
    client.return_value.config.cluster = {'server': CLUSTER_URL}

    monkeypatch.setattr('pykube.HTTPClient', client)

    return client


@pytest.mark.parametrize(
    'kwargs,ns,q,res',
    (
        ({}, [], [1, 2, 3], [1, 2, 3]),
        ({'namespace': 'default'}, [], [1, 2, 3], [1, 2, 3]),
        (
            {'namespace': None}, [{'metadata': {'name': 'n1'}}, {'metadata': {'name': 'n2'}}],
            [1, 2, 3], [1, 2, 3, 1, 2, 3]
        ),
    )
)
def test_get_resources(monkeypatch, kwargs, ns, q, res):
    client_mock(monkeypatch)

    query = MagicMock()
    query.filter.return_value = q

    namespaces = MagicMock()
    namespaces.return_value = ns

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper.namespaces', namespaces)

    k = KubernetesWrapper(**kwargs)
    result = k._get_resources(query)

    assert res == result

    if 'namespace' in kwargs and kwargs['namespace'] is None:
        calls = [call(namespace=n['metadata']['name']) for n in ns]
        query.filter.assert_has_calls(calls, any_order=True)
    else:
        query.filter.assert_called_with(namespace='default')


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'pod-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pod-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pod-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'pod-1', 'phase': 'Running', 'ready': True},
            {'selector': {'application': 'pod-1'}, 'field_selector': {'status.phase': 'Running'}},
            [resource_mock({'metadata': {'name': 'pod-1'}, 'spec': {}, 'status': {'phase': 'Running'}})]
        ),
        (
            {'name': 'pod-2', 'ready': False}, {'field_selector': {'metadata.name': 'pod-2'}},
            [resource_mock({'metadata': {'name': 'pod-2'}, 'spec': {}, 'status': {}}, ready=False)]
        ),
    ]
)
def test_pods(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    pod = MagicMock()
    query = pod.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Pod', pod)

    k = KubernetesWrapper()

    pods = k.pods(**kwargs)

    assert [r.obj for r in res] == pods

    get_resources.assert_called_with(query)
    pod.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize('kwargs', ({'ready': 1}, {'ready': 0}, {'phase': 'WRONG'}))
def test_pods_error(monkeypatch, kwargs):
    k = KubernetesWrapper()

    with pytest.raises(CheckError):
        k.pods(**kwargs)


def test_namespaces(monkeypatch):
    client_mock(monkeypatch)

    res = [resource_mock({'metadata': {}})]

    ns = MagicMock()
    ns.objects.return_value.all.return_value = res
    monkeypatch.setattr('pykube.Namespace', ns)

    k = KubernetesWrapper()
    namespaces = k.namespaces()

    assert [r.obj for r in res] == namespaces


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'node-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'node-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'node-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'beta.kubernetes.io/os': 'linux'}, {'selector': {'beta.kubernetes.io/os': 'linux'}},
            [resource_mock({'metadata': {'name': 'node-1'}, 'spec': {}, 'status': {'phase': 'Running'}})]
        ),
        (
            {'name': 'node-2'}, {'field_selector': {'metadata.name': 'node-2'}},
            [resource_mock({'metadata': {'name': 'node-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_nodes(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)

    node = MagicMock()
    node.objects.return_value.filter.return_value = res

    monkeypatch.setattr('pykube.Node', node)

    k = KubernetesWrapper()

    nodes = k.nodes(**kwargs)

    assert [r.obj for r in res] == nodes

    node.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'svc-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'svc-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'svc-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'svc-1'}, {'selector': {'application': 'svc-1'}},
            [resource_mock({'metadata': {'name': 'svc-1'}, 'spec': {}, 'status': {'phase': 'Running'}})]
        ),
        (
            {'name': 'svc-2'}, {'field_selector': {'metadata.name': 'svc-2'}},
            [resource_mock({'metadata': {'name': 'svc-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_services(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    service = MagicMock()
    query = service.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Service', service)

    k = KubernetesWrapper()

    services = k.services(**kwargs)

    assert [r.obj for r in res] == services

    get_resources.assert_called_with(query)
    service.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'ep-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ep-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ep-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'ep-1'}, {'selector': {'application': 'ep-1'}},
            [resource_mock({'metadata': {'name': 'ep-1'}, 'spec': {}, 'status': {'phase': 'Running'}})]
        ),
        (
            {'name': 'ep-2'}, {'field_selector': {'metadata.name': 'ep-2'}},
            [resource_mock({'metadata': {'name': 'ep-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_endpoints(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    endpoint = MagicMock()
    query = endpoint.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Endpoint', endpoint)

    k = KubernetesWrapper()

    endpoints = k.endpoints(**kwargs)

    assert [r.obj for r in res] == endpoints

    get_resources.assert_called_with(query)
    endpoint.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'ing-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ing-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ing-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'ing-1'}, {'selector': {'application': 'ing-1'}},
            [resource_mock({'metadata': {'name': 'ing-1'}, 'spec': {}, 'status': {'phase': 'Running'}})]
        ),
        (
            {'name': 'ing-2'}, {'field_selector': {'metadata.name': 'ing-2'}},
            [resource_mock({'metadata': {'name': 'ing-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_ingresses(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    ingress = MagicMock()
    query = ingress.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Ingress', ingress)

    k = KubernetesWrapper()

    ingresses = k.ingresses(**kwargs)

    assert [r.obj for r in res] == ingresses

    get_resources.assert_called_with(query)
    ingress.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'ss-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ss-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ss-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'ss-1', 'replicas': 2}, {'selector': {'application': 'ss-1'}},
            [resource_mock({'metadata': {'name': 'ss-1'}, 'spec': {}, 'status': {'replicas': '2'}}, replicas=2)]
        ),
        (
            {'name': 'ss-2'}, {'field_selector': {'metadata.name': 'ss-2'}},
            [resource_mock({'metadata': {'name': 'ss-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_statefulsets(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    statefulset = MagicMock()
    query = statefulset.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.StatefulSet', statefulset)

    k = KubernetesWrapper()

    statefulsets = k.statefulsets(**kwargs)

    assert [r.obj for r in res] == statefulsets

    get_resources.assert_called_with(query)
    statefulset.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'ds-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ds-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'ds-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'ds-1'}, {'selector': {'application': 'ds-1'}},
            [resource_mock({'metadata': {'name': 'ds-1'}, 'spec': {}, 'status': {}}, replicas=2)]
        ),
        (
            {'name': 'ds-2'}, {'field_selector': {'metadata.name': 'ds-2'}},
            [resource_mock({'metadata': {'name': 'ds-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_daemonsets(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    statefulset = MagicMock()
    query = statefulset.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.DaemonSet', statefulset)

    k = KubernetesWrapper()

    daemonsets = k.daemonsets(**kwargs)

    assert [r.obj for r in res] == daemonsets

    get_resources.assert_called_with(query)
    statefulset.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'rs-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'rs-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'rs-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'rs-1', 'replicas': 2}, {'selector': {'application': 'rs-1'}},
            [resource_mock({'metadata': {'name': 'rs-1'}, 'spec': {}, 'status': {'replicas': '2'}}, replicas=2)]
        ),
        (
            {'name': 'rs-2'}, {'field_selector': {'metadata.name': 'rs-2'}},
            [resource_mock({'metadata': {'name': 'rs-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_replicasets(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    replicaset = MagicMock()
    query = replicaset.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.ReplicaSet', replicaset)

    k = KubernetesWrapper()

    replicasets = k.replicasets(**kwargs)

    assert [r.obj for r in res] == replicasets

    get_resources.assert_called_with(query)
    replicaset.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'dep-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'dep-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'dep-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'dep-1', 'replicas': 2}, {'selector': {'application': 'dep-1'}},
            [resource_mock({'metadata': {'name': 'dep-1'}, 'spec': {}, 'status': {'replicas': '2'}}, replicas=2)]
        ),
        (
            {'name': 'dep-2'}, {'field_selector': {'metadata.name': 'dep-2'}},
            [resource_mock({'metadata': {'name': 'dep-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_deployments(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    deployment = MagicMock()
    query = deployment.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Deployment', deployment)

    k = KubernetesWrapper()

    deployments = k.deployments(**kwargs)

    assert [r.obj for r in res] == deployments

    get_resources.assert_called_with(query)
    deployment.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize('kwargs', ({'ready': 1}, {'ready': 0}))
def test_deployments_error(monkeypatch, kwargs):
    k = KubernetesWrapper()

    with pytest.raises(CheckError):
        k.deployments(**kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'cm-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'cm-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'cm-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'cm-1'}, {'selector': {'application': 'cm-1'}},
            [resource_mock({'metadata': {'name': 'cm-1'}, 'spec': {}, 'status': {'replicas': '2'}})]
        ),
        (
            {'name': 'cm-2'}, {'field_selector': {'metadata.name': 'cm-2'}},
            [resource_mock({'metadata': {'name': 'cm-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_configmaps(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    configmap = MagicMock()
    query = configmap.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.ConfigMap', configmap)

    k = KubernetesWrapper()

    configmaps = k.configmaps(**kwargs)

    assert [r.obj for r in res] == configmaps

    get_resources.assert_called_with(query)
    configmap.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'pvc-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pvc-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pvc-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'pvc-1', 'phase': 'Bound'}, {'selector': {'application': 'pvc-1'}},
            [resource_mock({'metadata': {'name': 'pvc-1'}, 'spec': {}, 'status': {'phase': 'Bound'}})]
        ),
        (
            {'name': 'pvc-2'}, {'field_selector': {'metadata.name': 'pvc-2'}},
            [resource_mock({'metadata': {'name': 'pvc-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_persistentvolumeclaims(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    persistentvolumeclaim = MagicMock()
    query = persistentvolumeclaim.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.PersistentVolumeClaim', persistentvolumeclaim)

    k = KubernetesWrapper()

    persistentvolumeclaims = k.persistentvolumeclaims(**kwargs)

    assert [r.obj for r in res] == persistentvolumeclaims

    get_resources.assert_called_with(query)
    persistentvolumeclaim.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'pv-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pv-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'pv-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'pv-1', 'phase': 'Bound'}, {'selector': {'application': 'pv-1'}},
            [resource_mock({'metadata': {'name': 'pv-1'}, 'spec': {}, 'status': {'phase': 'Bound'}})]
        ),
        (
            {'name': 'pv-2'}, {'field_selector': {'metadata.name': 'pv-2'}},
            [resource_mock({'metadata': {'name': 'pv-2'}, 'spec': {}, 'status': {}})]
        ),
    ]
)
def test_persistentvolumes(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)

    persistentvolume = MagicMock()
    query = persistentvolume.objects.return_value.filter.return_value
    query.all.return_value = res

    monkeypatch.setattr('pykube.PersistentVolume', persistentvolume)

    k = KubernetesWrapper()

    persistentvolumes = k.persistentvolumes(**kwargs)

    assert [r.obj for r in res] == persistentvolumes

    persistentvolume.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'compute-resources', 'namespace': 'default'},
                               'spec': {},
                               'status': {}}),
            ]
        ),
    ]
)
def test_resourcequotas(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)

    get_resources = get_resources_mock(res)

    resourcequota = MagicMock()
    query = resourcequota.objects.return_value.filter.return_value
    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.ResourceQuota', resourcequota)

    query.all.return_value = res

    k = KubernetesWrapper()

    res_quotas = k.resourcequotas(**kwargs)

    assert [r.obj for r in res] == res_quotas

    resourcequota.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'job-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'job-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'job-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'job-1', 'name': 'job-1'},
            {'selector': {'application': 'job-1'}, 'field_selector': {'metadata.name': 'job-1'}},
            [resource_mock({'metadata': {'name': 'job-1'}, 'spec': {}, 'status': {}})]
        ),
        (
            {'name': 'job-2'}, {'field_selector': {'metadata.name': 'job-2'}},
            [resource_mock({'metadata': {'name': 'job-2'}, 'spec': {}, 'status': {}}, ready=False)]
        ),
    ]
)
def test_jobs(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    job = MagicMock()
    query = job.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.Job', job)

    k = KubernetesWrapper()

    pods = k.jobs(**kwargs)

    assert [r.obj for r in res] == pods

    get_resources.assert_called_with(query)
    job.objects.return_value.filter.assert_called_with(**filter_kwargs)


@pytest.mark.parametrize(
    'kwargs,filter_kwargs,res',
    [
        (
            {}, {},
            [
                resource_mock({'metadata': {'name': 'cronjob-1'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'cronjob-2'}, 'spec': {}, 'status': {}}),
                resource_mock({'metadata': {'name': 'cronjob-3'}, 'spec': {}, 'status': {}}),
            ]
        ),
        (
            {'application': 'cronjob-1', 'name': 'cronjob-1'},
            {'selector': {'application': 'cronjob-1'}, 'field_selector': {'metadata.name': 'cronjob-1'}},
            [resource_mock({'metadata': {'name': 'cronjob-1'}, 'spec': {}, 'status': {}})]
        ),
        (
            {'name': 'cronjob-2'}, {'field_selector': {'metadata.name': 'cronjob-2'}},
            [resource_mock({'metadata': {'name': 'cronjob-2'}, 'spec': {}, 'status': {}}, ready=False)]
        ),
    ]
)
def test_cronjobs(monkeypatch, kwargs, filter_kwargs, res):
    client_mock(monkeypatch)
    get_resources = get_resources_mock(res)

    cronjob = MagicMock()
    query = cronjob.objects.return_value.filter.return_value

    monkeypatch.setattr(
        'zmon_worker_monitor.builtins.plugins.kubernetes.KubernetesWrapper._get_resources', get_resources)
    monkeypatch.setattr('pykube.CronJob', cronjob)

    k = KubernetesWrapper()

    pods = k.cronjobs(**kwargs)

    assert [r.obj for r in res] == pods

    get_resources.assert_called_with(query)
    cronjob.objects.return_value.filter.assert_called_with(**filter_kwargs)


def test_metrics(monkeypatch):
    client = client_mock(monkeypatch)

    resp = MagicMock()
    resp.text = 'metrics'

    client.return_value.session.get.return_value = resp

    parsed = MagicMock()
    parsed.samples = [
        ('metric-1', {}, 20.17), ('metric-2', {'verb': 'GET'}, 20.16), ('metric-1', {'verb': 'POST'}, 20.18)
    ]

    parser = MagicMock()
    parser.return_value = [parsed]

    monkeypatch.setattr('zmon_worker_monitor.builtins.plugins.kubernetes.text_string_to_metric_families', parser)

    k = KubernetesWrapper()
    metrics = k.metrics()

    expected = {
        'metric-1': [({}, 20.17), ({'verb': 'POST'}, 20.18)],
        'metric-2': [({'verb': 'GET'}, 20.16)],
    }

    assert metrics == expected

    parser.assert_called_with(resp.text)
    client.return_value.session.get.assert_called_with(CLUSTER_URL + '/metrics')

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import pykube

from collections import defaultdict

from pykube.objects import NamespacedAPIObject

from prometheus_client.parser import text_string_to_metric_families

from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from zmon_worker_monitor.zmon_worker.errors import CheckError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial

VALID_PHASE = ('Pending', 'Running', 'Failed', 'Succeeded', 'Unknown')

logger = logging.getLogger('zmon-worker.kubernetes-function')

# TODO Monkey-patch pykube to be compatible with Kubernetes 1.16.
# Drop if pykube is updated to a still maintained version.
pykube.objects.Deployment.version = 'apps/v1'
pykube.objects.StatefulSet.version = 'apps/v1'
pykube.objects.ReplicaSet.version = 'apps/v1'
pykube.objects.DaemonSet.version = 'apps/v1'
pykube.objects.CronJob.version = 'batch/v1beta1'


class PlatformCredentialsSet(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'platformcredentialssets'
    kind = 'PlatformCredentialsSet'


class AWSIAMRole(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'awsiamroles'
    kind = 'AWSIAMRole'


class Stack(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'stacks'
    kind = 'Stack'


class StackSet(NamespacedAPIObject):
    version = 'zalando.org/v1'
    endpoint = 'stacksets'
    kind = 'StackSet'


class KubernetesFactory(IFunctionFactoryPlugin):
    def __init__(self):
        super(KubernetesFactory, self).__init__()

    def configure(self, conf):
        """
        Called after plugin is loaded to pass the [configuration] section in their plugin info file
        :param conf: configuration dictionary
        """
        return

    def create(self, factory_ctx):
        """
        Automatically called to create the check function's object
        :param factory_ctx: (dict) names available for Function instantiation
        :return: an object that implements a check function
        """
        return propartial(KubernetesWrapper, check_id=factory_ctx['check_id'], __protected=['check_id'])


def _get_resources(object_manager, name=None, field_selector=None, **kwargs):
    if name is not None:
        if object_manager.namespace == pykube.all:
            raise CheckError("namespace is required for name= queries")

        if field_selector is not None or kwargs:
            raise CheckError("name= query doesn't support additional filters")

        try:
            return [object_manager.get_by_name(name)]
        except pykube.exceptions.ObjectDoesNotExist:
            return []

    filter_kwargs = {}

    if field_selector:
        filter_kwargs['field_selector'] = field_selector

    # labelSelector
    if kwargs:
        filter_kwargs['selector'] = kwargs

    return list(object_manager.filter(**filter_kwargs))


def _objects(objects):
    """Returns just the raw Kubernetes objects from a collection of objects"""
    return [o.obj for o in objects]


class KubernetesWrapper(object):
    def __init__(self, namespace='default', check_id='<unknown>'):
        self.__check_id = check_id
        self.__namespace = pykube.all if namespace is None else namespace

    @property
    def __client(self):
        config = pykube.KubeConfig.from_service_account()
        client = pykube.HTTPClient(config)
        client.session.headers['User-Agent'] = "{} (check {})".format(get_user_agent(), self.__check_id)
        client.session.trust_env = False
        return client

    def namespaces(self):
        """
        Return all namespaces.

        :return: List of namespaces.
        :rtype: list
        """
        return _objects(pykube.Namespace.objects(self.__client).all())

    def pods(self, name=None, phase=None, ready=None, **kwargs):
        """
        Return list of Pods.

        :param name: Pod name.
        :type name: str

        :param phase: Pod status phase. Valid values are: Pending, Running, Failed, Succeeded or Unknown.
        :type phase: str

        :param ready: Pod ready status. If None then all pods are returned.
        :type ready: bool

        :param **kwargs: Pod labelSelector filters. Example: application__in=['app-1', 'app-2'], version='v0.1'
                         Supported filter syntax:
                            - <label>
                            - <label>__in
                            - <label>__notin
                            - <label>__neq
        :type **kwargs: dict

        :return: List of pods. Typical pod has "metadata", "status" and "spec".
        :rtype: list
        """
        if ready is not None and type(ready) is not bool:
            raise CheckError('Invalid ready value.')

        if phase and phase not in VALID_PHASE:
            raise CheckError('Invalid phase. Valid phase values are {}'.format(VALID_PHASE))

        field_selector = None if phase is None else {'status.phase': phase}

        pods = _get_resources(pykube.Pod.objects(self.__client, self.__namespace),
                              name=name, field_selector=field_selector, **kwargs)

        return [pod.obj for pod in pods if ready is None or pod.ready == ready]

    def nodes(self, name=None, **kwargs):
        """
        Return list of nodes. Namespace is ignored.

        :param name: Node name.
        :type name: str

        :param **kwargs: Node labelSelector filters.
        :type **kwargs: dict

        :return: List of nodes. Typical pod has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.Node.objects(self.__client), name=name, **kwargs))

    def services(self, name=None, **kwargs):
        """
        Return list of Services.

        :param name: Service name.
        :type name: str

        :param **kwargs: Service labelSelector filters.
        :type **kwargs: dict

        :return: List of services. Typical service has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.Service.objects(self.__client, self.__namespace), name=name, **kwargs))

    def endpoints(self, name=None, **kwargs):
        """
        Return list of Endpoints.

        :param name: Endpoint name.
        :type name: str

        :param **kwargs: Endpoint labelSelector filters.
        :type **kwargs: dict

        :return: List of Endpoints. Typical Endpoint has "metadata", and "subsets".
        :rtype: list
        """
        return _objects(_get_resources(pykube.Endpoint.objects(self.__client, self.__namespace), name=name, **kwargs))

    def ingresses(self, name=None, **kwargs):
        """
        Return list of Ingresses.

        :param name: Ingress name.
        :type name: str

        :param **kwargs: Ingress labelSelector filters.
        :type **kwargs: dict

        :return: List of Ingresses. Typical Ingress has "metadata", "spec" and "status".
        :rtype: list
        """
        return _objects(_get_resources(pykube.Ingress.objects(self.__client, self.__namespace), name=name, **kwargs))

    def statefulsets(self, name=None, replicas=None, **kwargs):
        """
        Return list of Statefulsets.

        :param name: Statefulset name.
        :type name: str

        :param replicas: Statefulset replicas.
        :type replicas: int

        :param **kwargs: Statefulset labelSelector filters.
        :type **kwargs: dict

        :return: List of Statefulsets. Typical Statefulset has "metadata", "status" and "spec".
        :rtype: list
        """
        statfulsets = _get_resources(pykube.StatefulSet.objects(self.__client, self.__namespace),
                                     name=name, **kwargs)

        return [statfulset.obj for statfulset in statfulsets if replicas is None or statfulset.replicas == replicas]

    def daemonsets(self, name=None, **kwargs):
        """
        Return list of Daemonsets.

        :param name: Daemonset name.
        :type name: str

        :param **kwargs: Daemonset labelSelector filters.
        :type **kwargs: dict

        :return: List of Daemonsets. Typical Daemonset has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.DaemonSet.objects(self.__client, self.__namespace), name=name, **kwargs))

    def replicasets(self, name=None, replicas=None, **kwargs):
        """
        Return list of ReplicaSets.

        :param name: ReplicaSet name.
        :type name: str

        :param replicas: ReplicaSet replicas.
        :type replicas: int

        :param **kwargs: ReplicaSet labelSelector filters.
        :type **kwargs: dict

        :return: List of ReplicaSets. Typical ReplicaSet has "metadata", "status" and "spec".
        :rtype: list
        """
        replicasets = _get_resources(pykube.ReplicaSet.objects(self.__client, self.__namespace), name=name, **kwargs)

        return [replicaset.obj for replicaset in replicasets if replicas is None or replicaset.replicas == replicas]

    def deployments(self, name=None, replicas=None, ready=None, **kwargs):
        """
        Return list of Deployments.

        :param name: Deployment name.
        :type name: str

        :param replicas: Deployment replicas.
        :type replicas: int

        :param ready: Deployment ready status.
        :type ready: bool

        :param **kwargs: Deployment labelSelector filters.
        :type **kwargs: dict

        :return: List of Deployments. Typical Deployment has "metadata", "status" and "spec".
        :rtype: list
        """
        if ready is not None and type(ready) is not bool:
            raise CheckError('Invalid ready value.')

        deployments = _get_resources(pykube.Deployment.objects(self.__client, self.__namespace), name=name, **kwargs)

        return [
            deployment.obj for deployment in deployments
            if (replicas is None or deployment.replicas == replicas) and (ready is None or deployment.ready == ready)
        ]

    def configmaps(self, name=None, **kwargs):
        """
        Return list of ConfigMaps.

        :param name: ConfigMap name.
        :type name: str

        :param **kwargs: ConfigMap labelSelector filters.
        :type **kwargs: dict

        :return: List of ConfigMaps. Typical ConfigMap has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.ConfigMap.objects(self.__client, self.__namespace), name=name, **kwargs))

    def persistentvolumeclaims(self, name=None, phase=None, **kwargs):
        """
        Return list of PersistentVolumeClaims.

        :param name: PersistentVolumeClaim name.
        :type name: str

        :param phase: Volume phase.
        :type phase: str

        :param **kwargs: PersistentVolumeClaim labelSelector filters.
        :type **kwargs: dict

        :return: List of PersistentVolumeClaims. Typical PersistentVolumeClaim has "metadata", "status" and "spec".
        :rtype: list
        """
        pvcs = _get_resources(pykube.PersistentVolumeClaim.objects(self.__client, self.__namespace),
                              name=name, **kwargs)

        return [pvc.obj for pvc in pvcs if phase is None or pvc.obj['status'].get('phase') == phase]

    def persistentvolumes(self, name=None, phase=None, **kwargs):
        """
        Return list of PersistentVolumes.

        :param name: PersistentVolume name.
        :type name: str

        :param phase: Volume phase.
        :type phase: str

        :param **kwargs: PersistentVolume labelSelector filters.
        :type **kwargs: dict

        :return: List of PersistentVolumes. Typical PersistentVolume has "metadata", "status" and "spec".
        :rtype: list
        """
        pvs = _get_resources(pykube.PersistentVolume.objects(self.__client), name=name, **kwargs)

        return [vc.obj for vc in pvs if phase is None or vc.obj['status'].get('phase') == phase]

    def jobs(self, name=None, **kwargs):
        """
        Return list of Jobs.

        :param name: Job name.
        :type name: str

        :param **kwargs: Job labelSelector filters.
        :type **kwargs: dict

        :return: List of Jobs. Typical Job has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.Job.objects(self.__client, self.__namespace), name=name, **kwargs))

    def cronjobs(self, name=None, **kwargs):
        """
        Return list of CronJobs.

        :param name: CronJob name.
        :type name: str

        :param **kwargs: CronJob labelSelector filters.
        :type **kwargs: dict

        :return: List of CronJobs. Typical CronJob has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.CronJob.objects(self.__client, self.__namespace), name=name, **kwargs))

    def platformcredentialssets(self, name=None, **kwargs):
        """
        Return list of PlatformCredentialsSets.

        :param name: PlatformCredentialsSet name.
        :type name: str

        :param **kwargs: PlatformCredentialsSet labelSelector filters.
        :type **kwargs: dict

        :return: List of PlatformCredentialsSets. Typical PlatformCredentialsSet has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(PlatformCredentialsSet.objects(self.__client, self.__namespace),
                                       name=name, **kwargs))

    def awsiamroles(self, name=None, **kwargs):
        """
        Return list of AWSIAMRoles.

        :param name: AWSIAMRole name.
        :type name: str

        :param **kwargs: AWSIAMRole labelSelector filters.
        :type **kwargs: dict

        :return: List of AWSIAMRoles. Typical AWSIAMRole has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(AWSIAMRole.objects(self.__client, self.__namespace), name=name, **kwargs))

    def stacksets(self, name=None, **kwargs):
        """
        Return list of StackSets.

        :param name: StackSet name.
        :type name: str

        :param **kwargs: StackSet labelSelector filters.
        :type **kwargs: dict

        :return: List of StackSets. Typical StackSet has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(StackSet.objects(self.__client, self.__namespace), name=name, **kwargs))

    def stacks(self, name=None, **kwargs):
        """
        Return list of Stacks.

        :param name: Stack name.
        :type name: str

        :param **kwargs: Stack labelSelector filters.
        :type **kwargs: dict

        :return: List of Stacks. Typical Stack has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(Stack.objects(self.__client, self.__namespace), name=name, **kwargs))

    def metrics(self):
        """
        Return API server metrics in prometheus format.

        :return: Cluster metrics.
        :rtype: dict
        """
        url = self.__client.config.cluster['server'] + '/metrics'
        response = self.__client.session.get(url)

        response.raise_for_status()

        samples_by_name = defaultdict(list)

        for l in text_string_to_metric_families(response.text):
            for s in l.samples:
                samples_by_name[s[0]].append((s[1], s[2]))

        return samples_by_name

    def resourcequotas(self, name=None, **kwargs):
        """
        Return list of resource quotas.

        :param name: quota name.
        :type name: str

        :param **kwargs: resourceQuota labelSelector filters.
        :type **kwargs: dict

        :return: List of resourceQuota. Typical resourceQuota has "metadata", "status" and "spec".
        :rtype: list
        """
        return _objects(_get_resources(pykube.ResourceQuota.objects(self.__client, self.__namespace),
                                       name=name, **kwargs))

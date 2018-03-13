#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import pykube

from collections import defaultdict

from prometheus_client.parser import text_string_to_metric_families

from zmon_worker_monitor.zmon_worker.errors import CheckError

from zmon_worker_monitor.adapters.ifunctionfactory_plugin import IFunctionFactoryPlugin, propartial


VALID_PHASE = ('Pending', 'Running', 'Failed', 'Succeeded', 'Unknown')

logger = logging.getLogger('zmon-worker.kubernetes-function')


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
        return propartial(KubernetesWrapper)


class KubernetesWrapper(object):
    def __init__(self, namespace='default'):
        self.__namespace = namespace

    @property
    def __client(self):
        config = pykube.KubeConfig.from_service_account()
        client = pykube.HTTPClient(config)
        client.session.trust_env = False
        return client

    def _get_filter_kwargs(self, name=None, phase=None, **kwargs):
        filter_kwargs = {}
        field_selector = {}

        if phase:
            field_selector['status.phase'] = phase

        if name:
            field_selector['metadata.name'] = name

        if field_selector:
            filter_kwargs['field_selector'] = field_selector

        # labelSelector
        if kwargs:
            filter_kwargs['selector'] = kwargs

        return filter_kwargs

    def _get_resources(self, query):
        """
        Return the resource query after filtering with desired namespace(s).

        :param query: Pykube resource query.
        :type query: pykube.query.Query

        :return: List of pykube resources.
        :rtype: list
        """
        resources = []

        # check if we need resources for all namespaces.
        if self.__namespace is None:
            namespaces = self.namespaces()

            for namespace in namespaces:
                ns = namespace['metadata']['name']
                resources += list(query.filter(namespace=ns))
        else:
            resources = list(query.filter(namespace=self.__namespace))

        return resources

    def namespaces(self):
        """
        Return all namespaces.

        :return: List of namespaces.
        :rtype: list
        """
        return [ns.obj for ns in pykube.Namespace.objects(self.__client).all()]

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

        filter_kwargs = self._get_filter_kwargs(name, phase, **kwargs)

        query = pykube.Pod.objects(self.__client).filter(**filter_kwargs)

        pods = self._get_resources(query)

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        return [n.obj for n in pykube.Node.objects(self.__client).filter(**filter_kwargs)]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.Service.objects(self.__client).filter(**filter_kwargs)

        services = self._get_resources(query)

        return [service.obj for service in services]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.Endpoint.objects(self.__client).filter(**filter_kwargs)

        endpoints = self._get_resources(query)

        return [endpoint.obj for endpoint in endpoints]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.Ingress.objects(self.__client).filter(**filter_kwargs)

        ingresses = self._get_resources(query)

        return [ingress.obj for ingress in ingresses]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.StatefulSet.objects(self.__client).filter(**filter_kwargs)

        statfulsets = self._get_resources(query)

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.DaemonSet.objects(self.__client).filter(**filter_kwargs)

        daemonsets = self._get_resources(query)

        return [daemonset.obj for daemonset in daemonsets]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.ReplicaSet.objects(self.__client).filter(**filter_kwargs)

        replicasets = self._get_resources(query)

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

        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.Deployment.objects(self.__client).filter(**filter_kwargs)

        deployments = self._get_resources(query)

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.ConfigMap.objects(self.__client).filter(**filter_kwargs)

        configmaps = self._get_resources(query)

        return [configmap.obj for configmap in configmaps]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.PersistentVolumeClaim.objects(self.__client).filter(**filter_kwargs)

        pvcs = self._get_resources(query)

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.PersistentVolume.objects(self.__client).filter(**filter_kwargs)

        # PersistentVolume does not belong to a "namespace". Calling with **default** will fail with 404.
        pvs = query.all()

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.Job.objects(self.__client).filter(**filter_kwargs)

        jobs = self._get_resources(query)

        return [job.obj for job in jobs]

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
        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)

        query = pykube.CronJob.objects(self.__client).filter(**filter_kwargs)

        cronjobs = self._get_resources(query)

        return [job.obj for job in cronjobs]

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

        filter_kwargs = self._get_filter_kwargs(name=name, **kwargs)
        query = pykube.ResourceQuota.objects(self.__client).filter(**filter_kwargs)
        qs = self._get_resources(query)
        return [q.obj for q in qs]

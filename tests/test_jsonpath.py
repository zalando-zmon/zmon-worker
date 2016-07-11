import pytest

from zmon_worker_monitor.zmon_worker.tasks.main import jsonpath_flat_filter


@pytest.fixture(params=[
    (
        {
            'obj': {'result': {'status': 'SUCCESS', 'count': 10}},
            'path': '*.status'
        },
        {'result.status': 'SUCCESS'}
    ),
    (
        {
            'obj': {
                'results': [
                    {'result': {'status': 'SUCCESS', 'count': 10}},
                    {'result': {'status': 'FAILED', 'count': 10}},
                    {'result': {'status': 'SUCCESS', 'count': 10}},
                    {'result': {'status': 'FAILED', 'count': 10}},
                ],
                'query': {'status': 'SUCCESS'}
            },
            'path': 'results.[*].*.status'
        },
        {
            'results.[0].result.status': 'SUCCESS',
            'results.[1].result.status': 'FAILED',
            'results.[2].result.status': 'SUCCESS',
            'results.[3].result.status': 'FAILED',
        }
    )
])
def fx_jsonpath(request):
    return request.param


def test_jsonpath_flat_filter(fx_jsonpath):
    kwargs, exp = fx_jsonpath

    res = jsonpath_flat_filter(**kwargs)

    assert res == exp

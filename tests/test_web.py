import os
import json
import re
import ConfigParser

from httpretty import HTTPretty
from httpretty import httprettified

from nose.tools import assert_equal

import ckanexporterservice.main as main

os.environ['JOB_CONFIG'] = os.path.join(os.path.dirname(__file__),
                                        'test.ini')

app = main.serve_test()


def _normalize_newlines(string):
    return re.sub(r'(\r\n|\r|\n)', '\n', string)


def join_static_path(filename):
    return os.path.join(os.path.dirname(__file__), 'static', filename)


def get_static_file(filename):
    return open(join_static_path(filename)).read()


class TestWeb():
    @classmethod
    def setup_class(cls):
        config = ConfigParser.ConfigParser()
        config_file = os.environ.get('JOB_CONFIG')
        config.read(config_file)

    def test_status(self):
        rv = app.get('/status')
        assert json.loads(rv.data) == dict(version=0.1,
                                           job_types=['export_as_csv'],
                                           name='ckan_exporter'), rv.data

    @httprettified
    def test_export_csv(self):
        url = 'http://www.ckan.org/api/action/datastore_search?resource_id=3c32eff-273c-413a-a8d4-69433d02c943'
        HTTPretty.register_uri(HTTPretty.GET, url,
                           body=get_static_file('simple.json'),
                           content_type="application/json")

        rv = app.post('/job',
                      data=json.dumps({
                            "job_type": "export_as_csv",
                            "data": {
                                "ckan_url": "http://www.ckan.org/",
                                "resource_id": "3c32eff-273c-413a-a8d4-69433d02c943"
                            },
                            "metadata": {
                                "mimetype": "text/csv"
                            }
                            }),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert 'job_id' in return_data
        assert return_data['status'] == 'complete', return_data['error']
        assert_equal(
            _normalize_newlines(return_data['data']),
            _normalize_newlines(get_static_file('simple.csv'))
            )

        rv = app.get('/job/{0}/data'.format(return_data['job_id']))
        assert 'text/csv' in rv.content_type
        assert_equal(
            _normalize_newlines(rv.data),
            _normalize_newlines(get_static_file('simple.csv'))
            )

    @httprettified
    def test_export_long_csv(self):
        url = 'http://www.ckan.org/api/action/datastore_search?resource_id=3c32eff-273c-413a-a8d4-69433d02c943'
        HTTPretty.register_uri(HTTPretty.GET, url,
                            responses=[
                               HTTPretty.Response(body=get_static_file('long.json'), status=200),
                               HTTPretty.Response(body=get_static_file('long_2.json'), status=200)
                            ],
                            content_type="application/json")

        rv = app.post('/job',
                      data=json.dumps({
                            "job_type": "export_as_csv",
                            "data": {
                                "ckan_url": "http://www.ckan.org/",
                                "resource_id": "3c32eff-273c-413a-a8d4-69433d02c943"
                            },
                            "metadata": {
                                "mimetype": "text/csv"
                            }
                        }),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert 'job_id' in return_data
        assert return_data['status'] == 'complete', return_data['error']
        assert_equal(return_data['data'].count('\n'), 201)
        assert '1\r' in return_data['data']
        assert '199\r' in return_data['data']

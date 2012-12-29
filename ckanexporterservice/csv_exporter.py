import csv
import StringIO

import requests

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util


def urljoin(*args):
    return "/".join(map(lambda x: str(x).strip('/'), args))


def fetch_from(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise util.JobError(
            'CKAN DataStorer responded with status code {0} and content {1}'.format(response.status_code, response.text))

    data = response.json
    if not data:
        raise util.JobError('CKAN DataStorer response cannot be parsed.')

    return data


@job.sync
def export_as_csv(task_id, provided):
    provided = provided['data']
    resource_url = urljoin(
        provided['ckan_url'],
        '/api/action/datastore_search?resource_id={res_id}'.format(res_id=provided['resource_id']))

    data = fetch_from(resource_url)

    total = data['result']['total']

    # create a csv writer
    f = StringIO.StringIO()
    wr = csv.writer(f)

    # write header
    header = [x['id'] for x in data['result']['fields']]
    wr.writerow(header)

    # write data and fetch more, if necessary
    written = 0
    while written < total:
        #print resource_url
        records = data['result']['records']
        for record in records:
            wr.writerow([record[column] for column in header])
        written += len(records)

        next_url = data['result']['_links']['next']
        resource_url = urljoin(provided['ckan_url'], next_url)
        data = fetch_from(resource_url)

    f.seek(0)
    return f.read()

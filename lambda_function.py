import os
import json
import gzip
import base64
from pprint import pprint
from datetime import datetime
from elasticsearch import Elasticsearch, helpers


default_vars = {
	'index_name': '{0}-{1}'.format(os.environ.get('es_index'), datetime.utcnow().strftime('%d-%m-%y')),
	'index_doc_type': 'doc',
	'result': {'success': 0, 'failed': 0},
	'status_code': {'success': 200, 'failed': 601}
}


def lambda_handler(event, context):
	# TODO implement
	data = print('======== Cloudwatch log received')
	if event and event.get('awslogs') and event.get('awslogs').get('data'):
		es = Elasticsearch(['https://{0}:{1}@{2}:{3}'.format(os.environ.get('es_user'), os.environ.get('es_pwd'), os.environ.get('es_host'), os.environ.get('es_port'))], use_ssl=True, verify_certs=False)
		print('======== Connection to ES established')
		send_to_es(es, get_json_logs(event.get('awslogs').get('data')))
	return {
		'status_code': default_vars.get('status_code').get('success') if default_vars.get('result').get('failed') == 0 else default_vars.get('status_code').get('failed'),
		'body': default_vars.get('result')
	}


def send_to_es(es, logs, chunk_size=100, max_retries=5, initial_backoff=2):
	global default_vars
	# helpers.bulk(es, logs)
	success, failed = 0, 0

	# list of errors to be collected is not stats_only
	errors = []

	for ok, item in helpers.streaming_bulk(es, logs, chunk_size=chunk_size, max_retries=max_retries, initial_backoff=initial_backoff):
		# go through request-reponse pairs and detect failures
		if not ok:
			if not stats_only:
				errors.append(item)
			failed += 1
		else:
			success += 1

	print('======== Success: {0}'.format(success))
	print('======== Failed: {0}'.format(failed))

	if failed > 0:
		print('======== All failed items are:')
		print(errors)

	default_vars['result']['success'] = success
	default_vars['result']['failed'] = failed


def get_json_logs(cloudwatch_log):
	data = base64.b64decode(cloudwatch_log)
	# print('======== Base64 decoding complete')
	data = json.loads((gzip.decompress(data)).decode())
	# print('======== Data is now in JSON format')
	for i in data.get('logEvents'):
		yield {
			"_index": default_vars.get('index_name'),
			"_type": default_vars.get('index_doc_type'),
			"_source": json.loads(i.get('message'))
		}
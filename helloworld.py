import webapp2
import logging
import uuid
import json
from google.appengine.ext import vendor

# Add any libraries installed in the "lib" folder.
vendor.add('lib')

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

class MainPage(webapp2.RequestHandler):
    def get(self):
        self.response.headers['Content-Type'] = 'text/plain'
        self.response.write('Hello, World!')

class M2M(webapp2.RequestHandler):
    def post(self):
	logging.info('Starting M2M handler')

	logging.info('===================')
	logging.info('self.request.body')
	logging.info(self.request.body)
	logging.info('===================')

	ericsson_dummy = json.loads(self.request.body)
	bigquery_payload = {}
	for ed in ericsson_dummy:
		bigquery_payload[ed['resourceSpec']] = ed['value']
		bigquery_payload[u'gatewayId'] = ed['gatewayId']
		bigquery_payload[u'timestamp'] = ed['timestamp']


	logging.info('===================')
	logging.info('bigquery_payload')
	logging.info(bigquery_payload)
	logging.info('===================')
	
	
	project_id = 'internet-of-food--red-team'
	dataset_id = 'ericsson_iot_data_test_us'
	table_name = 'prod'
	credentials = GoogleCredentials.get_application_default()
	bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
	insert_all_data = {
	        'insertId': str(uuid.uuid4()),
	        'rows': [{'json': [bigquery_payload] }]
	}
	bigquery_response = bigquery.tabledata().insertAll(
        	projectId=project_id,
        	datasetId=dataset_id,
        	tableId=table_name,
        	body=insert_all_data
	).execute(num_retries=2)
	logging.info('===================')
	logging.info('bigquery_response')
	logging.info(bigquery_response)
	logging.info('===================')
	

    def get(self):
	challenge = self.request.get('hub.challenge', '')
	logging.info('challenge:' + challenge)
	self.response.headers['Content-Type'] = 'text/plain'
	self.response.write(challenge)

app = webapp2.WSGIApplication([
    ('/', MainPage), ('/m2m', M2M),
], debug=True)


import requests
import xmltodict

from simple_salesforce.api import Salesforce
from simple_salesforce.login import SalesforceLogin
from simple_salesforce.api import SalesforceGeneralError


class BulkSalesforce(Salesforce):

    def __init__(self,**kwargs):
        super(BulkSalesforce, self).__init__(**kwargs)

        self.job_id = None
        self.job_url = None
        self.batch_ids = []

        self.base_url = ('https://{instance}/services/async/{version}/job'
                         .format(instance=self.sf_instance,
                                 version=self.sf_version))

        self.bulk_headers = {
            'Content-Type' : 'application/xml',
            'X-SFDC-Session' : self.session_id,
        }


    def initiate_bulk_job(self, sfobject_type, job_type, concurrency_mode='Parallel', batch_content_type='CSV'):

        open_job_xml = '''<?xml version="1.0" encoding="UTF-8"?>
                    <jobInfo 
                      xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                      <operation>{operation}</operation>
                      <object>{object}</object>
                      <concurrencyMode>{concurrency_mode}</concurrencyMode>
                      <contentType>{content_type}</contentType>
                    </jobInfo>'''.format(operation=job_type,
                                         object=sfobject_type,
                                         concurrency_mode=concurrency_mode,
                                         content_type=batch_content_type)

        response = requests.post(self.base_url, headers=self.bulk_headers, data=open_job_xml)

        response_data = xmltodict.parse(response.text)

        self.job_id = response_data['jobInfo']['id']

        self.job_url = "{base_url}/{job_id}/batch".format(
                                    base_url=self.base_url,
                                    job_id=self.job_id)


    
    def add_batch_to_job(self, bulk_data_chunk):
        
        #change content type header for the add requests
        # can be csv or xml (xml can also contain binary data)
        self.bulk_headers['Content-Type'] = 'text/csv'
        
        response = requests.post(self.job_url, headers=self.bulk_headers, data=bulk_data_chunk)

        batch_add_response_data = xmltodict.parse(response.text)

        self.batch_ids.append(batch_add_response_data['batchInfo']['id'])

        return batch_add_response_data


    def close_bulk_job(self):
        close_job_xml = '''<?xml version="1.0" encoding="UTF-8"?>
                    <jobInfo 
                      xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                      <state>Closed</state>
                    </jobInfo>'''

        self.bulk_headers['Content-Type'] = 'application/xml'
        
        response = requests.post(self.job_url, headers=self.bulk_headers, data=close_job_xml)

        return xmltodict.parse(response.text)



    def check_batch_status(self, batch_id=None):
        #can check individual batch or all batches in a job

        if batch_id is not None: #get status of a specific batch
            
            status_url = "{base_url}/{job_id}/batch/{batch_id}".format(
                                        base_url=self.base_url,
                                        job_id=self.job_id,
                                        batch_id=batch_id)
        else: # get job status summary

            status_url = "{base_url}/{job_id}/batch/".format(
                                        base_url=self.base_url,
                                        job_id=self.job_id)
            
        response = requests.get(status_url, headers=self.bulk_headers)

        status_data = xmltodict.parse(response.text)

        return status_data


    def get_batch_results(self, batch_id=None):

        if batch_id is not None: #get results from specific batch
            
            batch_result = None

            results_url = "{base_url}/{job_id}/batch/{batch_id}".format(
                                        base_url=self.base_url,
                                        job_id=self.job_id,
                                        batch_id=batch_id)
            
            response = requests.get(results_url, headers=self.bulk_headers)

            batch_result = xmltodict.parse(response.text)

            return batch_result           
        
        else: # get job status summary

            job_results = []

            for b_id in self.batch_ids:

                results_url = "{base_url}/{job_id}/batch/{batch_id}/result".format(
                                            base_url=self.base_url,
                                            job_id=self.job_id,
                                            batch_id=b_id)

                response = requests.get(results_url, headers=self.bulk_headers)

                #f_buffer = StringIO.StringIO(response.text)

                job_results.append((b_id, response.text))

            return job_results







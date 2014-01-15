"""Utility functions for simple-salesforce"""

import xml.dom.minidom

import csv
import cStringIO
import datetime
import time

def getUniqueElementValueFromXmlString(xmlString, elementName):
    """
    Extracts an element value from an XML string.

    For example, invoking
    getUniqueElementValueFromXmlString('<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', 'foo')
    should return the value 'bar'.
    """
    xmlStringAsDom = xml.dom.minidom.parseString(xmlString)
    elementsByName = xmlStringAsDom.getElementsByTagName(elementName)
    elementValue = None
    if len(elementsByName) > 0:
        elementValue = elementsByName[0].toxml().replace('<' + elementName + '>','').replace('</' + elementName + '>','')
    return elementValue



def bulk_csv_batch_maker(filename, batch_size=5000):
    '''
    yields csv formated strings suitable to include in an http request with row count = batch_size
    '''

    job_f = open(filename, 'rb')
    job_csv = csv.reader(job_f)

    batch_header = None
    
    batch_buffer = cStringIO.StringIO()
    batch_buffer_csv = csv.writer(batch_buffer)
    
    for job_rnum, job_row in enumerate(job_csv):
        
        if job_rnum == 0: #set the header var and header of first batch
            batch_header = job_row
            batch_buffer_csv.writerow(batch_header)

        if (job_rnum % batch_size == 0 and job_rnum > 0):
            yield batch_buffer.getvalue()
            batch_buffer.close() #close and empty StringIO, then start a new one
            batch_buffer = cStringIO.StringIO()
            batch_buffer_csv = csv.writer(batch_buffer)
            batch_buffer_csv.writerow(batch_header)

        batch_buffer_csv.writerow(job_row)
    
    yield batch_buffer.getvalue()


def bulk_batch_monitor(bulk_sf_obj, check_interval=20):
    '''
    checks the status report of a bulk salesforce job and reports progress of job until complete

    by default will check every 20 seconds
    '''

    batch_statuses = bulk_sf_obj.check_batch_status()

    batch_data = {}
    batch_data['queued_count'] = 0
    batch_data['complete_count'] = 0
    batch_data['inprogress_count'] = 0
    
    for b_stat in batch_statuses['batchInfoList']['batchInfo']:
        if b_stat['state'] == 'Queued':
            batch_data['queued_count'] += 1            
        elif b_stat['state'] == 'InProgress':
            batch_data['inprogress_count'] += 1
        elif b_stat['state'] == 'Completed':
            batch_data['complete_count'] += 1

    if batch_data['queued_count'] + batch_data['inprogress_count'] > 0:
        print "[{}] complete={} inprogress={} queued={}".format(
                datetime.datetime.now(),
                batch_data['complete_count'],
                batch_data['inprogress_count'],
                batch_data['queued_count'],
                )
        time.sleep(check_interval)
        return bulk_batch_monitor(bulk_sf_obj)
    else:
        print "[{}] job complete".format(datetime.datetime.now())
        return batch_data    
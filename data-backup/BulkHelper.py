# -*- coding: utf-8 -*-

from salesforce_bulk import SalesforceBulk
from salesforce_bulk.util import IteratorBytesIO
import math
from config import Config
from SchemaHelper import SchemaHelper
from IOHelper import IOHelper
import time

class BulkHelper:

    __instance = None
    __bulk = None

    @staticmethod
    def getInstance():
        if BulkHelper.__instance == None:
            BulkHelper()
        return BulkHelper.__instance

    def __init__(self):
        if BulkHelper.__instance != None:
            raise Exception("BulkHelper class is a singleton!")
        else:
            BulkHelper.__instance = self
            self.__bulk = SalesforceBulk(username = Config.USERNAME,
                password=Config.PASSWORD, security_token=Config.SECURITY_TOKEN,
                sandbox = Config.IS_SANDBOX, API_version = Config.API_VERSION)

    def getQueryBatchResultIds(self, batchId, jobId):
        return self.__bulk.get_query_batch_result_ids(batchId, jobId)

    def closeJob(self, jobId):
        self.__bulk.close_job(jobId)

    def getQueryBatchResults(self, batchId, resultId, jobId, raw=True):
        return self.__bulk.get_query_batch_results(batchId, resultId, jobId, raw=True)

    def getBatchList(self, jobId):
        return self.__bulk.get_batch_list(jobId)

    def getBatchStatus(self, batchId, jobId, reload):
        return self.__bulk.batch_status(batchId, jobId, reload)

    def createNormalBatch(self, objectName, fieldList, recordCount):
    	# create a queryall job
        job = self.__bulk.create_queryall_job(objectName, contentType='CSV')
        print("job: " + str(job))
        # create batches for each pagination
        batch_list = []
        for chunk in range(math.ceil(recordCount / Config.CHUNK_SIZE)):
            str_query = "SELECT {} FROM {} LIMIT {} OFFSET {}".format(
                    ', '.join(fieldList), objectName,Config.CHUNK_SIZE,
                    chunk * Config.CHUNK_SIZE)
            # add a batch to the job
            batch = self.__bulk.query(job, str_query)
            print("batch: " + str(batch))
            batch_list.append(batch)
        return job, batch_list

    def createChunkBatch(self, objectName, fieldList):
        # create a queryall job with PK Chunking enabled
    	job = self.__bulk.create_queryall_job(objectName, contentType='CSV',
    		pk_chunking=Config.CHUNK_SIZE)
    	print("job: " + str(job))
    	# add a batch to the job
    	str_query = "SELECT {} FROM {}".format(', '.join(fieldList), objectName)
    	main_batch = self.__bulk.query(job, str_query)
    	print("main batch: " + str(main_batch))
    	return job, main_batch

    def createNormalBatch(self, objectName, fieldList):
    	# create a queryall job with PK Chunking enabled
    	job = self.__bulk.create_queryall_job(objectName, contentType='CSV')
    	print("job: " + str(job))
    	# add a batch to the job
    	str_query = "SELECT {} from {}".format(', '.join(fieldList), objectName)
    	batch = self.__bulk.query(job, str_query)
    	print("batch: " + str(batch))
    	return job, batch


if __name__ == "__main__":
    IOHelper.init()
    schemaHelper = SchemaHelper.getInstance()
    object_fields_dict, object_chunkable_dict = schemaHelper.getObjectFieldDict()
    obj_record_count_dict = schemaHelper.getObjectRecordCount(object_fields_dict)

    bulkHelper = BulkHelper.getInstance()
    bulkHelper.createNormalBatch('Account', object_fields_dict['Account'], obj_record_count_dict['Account'])

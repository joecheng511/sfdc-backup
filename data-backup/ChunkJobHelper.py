# -*- coding: utf-8 -*-

import threading
import time
from BulkHelper import BulkHelper
from IOHelper import IOHelper
from AzureHelper import AzureHelper
from config import Config

class ChunkJobHelper:

	__instance = None
	chunk_job_mainbatch_list = []
	chunk_job_object_dict = {}

	@staticmethod
	def getInstance():
		if ChunkJobHelper.__instance == None:
			ChunkJobHelper()
		return ChunkJobHelper.__instance

	def __init__(self):
		if ChunkJobHelper.__instance != None:
			raise Exception("ChunkJobHelper class is a singleton!")
		else:
			ChunkJobHelper.__instance = self
			self.__bulkHelper = BulkHelper.getInstance()

	def createJob(self, objectName, fieldList):
		print("###### Sending retrieve request: {} ".format(objectName))
		try:
			job_id, main_batch_id = self.__bulkHelper.createChunkBatch(objectName, fieldList)
			self.chunk_job_mainbatch_list.append({"job":job_id, "batch": main_batch_id})
			self.chunk_job_object_dict[job_id] = objectName
			IOHelper.appendToLog("chunk_job.log",
					"\nobject: {}, job_id: {}, main_batch_id, {}".format(
					objectName, job_id, main_batch_id)
			)
		except Exception as inst:
			print( inst )
			IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))

	def startCheckChunkJobStatusThread(self):
		chunkJobStatusCheckThread = ChunkJobStatusCheckThread(self)
		chunkJobStatusCheckThread.start()


class ChunkJobStatusCheckThread (threading.Thread):

	chunk_notprocessed_job_list = []

	def __init__(self, chunkHelper):
		if not isinstance(chunkHelper, ChunkJobHelper):
			raise Exception("Variable chunkHelper must be an instance of ChunkJobHelper!")
		self.__chunkHelper = chunkHelper
		self.__bulkHelper = BulkHelper.getInstance()
		threading.Thread.__init__(self)

	def run(self):
		print( "#### Starting Chunk Job Status Checking Thread" )
		self._check_chunk_main_batch_status()
		print( "#### Exiting Chunk Job Status Checking Thread" )

	def _check_chunk_main_batch_status(self):
		while len(self.__chunkHelper.chunk_job_mainbatch_list) > 0:
			print("\n\n### Checking chunk main batch status...")
			print("# chunk_job_mainbatch_list: {}".format(self.__chunkHelper.chunk_job_mainbatch_list))
			for job_batch_dict in self.__chunkHelper.chunk_job_mainbatch_list[:]:
				j_id, b_id = job_batch_dict['job'], job_batch_dict['batch']
				try:
					b_state = self.__bulkHelper.getBatchStatus(b_id, j_id, True)['state']
					object_name = self.__chunkHelper.chunk_job_object_dict[j_id]
					print( "# chunk job: {} (object: {}), batch: {} status: {}".format(j_id, object_name, b_id, b_state) )
					if b_state == "NotProcessed":
						self.chunk_notprocessed_job_list.append(j_id)
						self.__chunkHelper.chunk_job_mainbatch_list.remove(job_batch_dict)
						print( "# chunk_notprocessed_job_list: {}".format(', '.join(self.chunk_notprocessed_job_list)) )
						download_chunk_batch_result_thread = DownloadChunkBatchResultThread("Download Chunk Batch Result Thread", object_name, j_id, b_id)
						download_chunk_batch_result_thread.start()
					elif b_state == "Failed":
						IOHelper.appendToLog("extract_error.log", "\n\n# chunk job: {} (object: {}), batch: {} status: {}".format(j_id, object_name, b_id, b_state))
						chunk_job_mainbatch_list.remove(job_batch_dict)
				except Exception as inst:
					print( inst )
					IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
				time.sleep(1)
			time.sleep(40)


class DownloadChunkBatchResultThread (threading.Thread):

	def __init__(self, name, objectName, jobId, mainBatchId):
		threading.Thread.__init__(self)
		self.name = name
		self.objectName = objectName
		self.jobId = jobId
		self.mainBatchId = mainBatchId
		self.__bulkHelper = BulkHelper.getInstance()
		self.__azureHelper = AzureHelper.getInstance()

	def run(self):
		print( "#### Starting " + self.name )
		self._download_chunk_batch_result(self.objectName, self.jobId, self.mainBatchId)
		print( "#### Exiting " + self.name )

	def _download_chunk_batch_result(self, object_name, job, main_batch):
		# wait until main_batch become "NotProcessed"
		while True:
			try:
				main_batch_state = self.__bulkHelper.getBatchStatus(main_batch, job, True)['state']
				print( "\n# Checking chunk job: {} (object: {}) - main_batch status: {}".
						format(job, object_name, main_batch_state) )
				if main_batch_state == "NotProcessed":
					break
				else:
					time.sleep(40)
			except Exception as inst:
				print( inst )
				IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
		# wait until all the sub-batches become "Completed"
		while True:
			try:
				sub_batches = self.__bulkHelper.getBatchList(job)
				can_break = True
				print( "\n# Checking chunk job: {} (object: {}) - sub_batch status:".format(job, object_name) )
				for b in sub_batches:
					if b['id'] != main_batch:
						print( "sub_batch: " + b['id'] + " - state: " + b['state'] )
						if b['state'] != "Completed":
							can_break = False
				if can_break:
					print( "All sub_batches completed!" )
					break
				else:
					time.sleep(40)
			except Exception as inst:
				print( inst )
				IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
		try:
			sub_batch_ids = [b['id'] for b in self.__bulkHelper.getBatchList(job) if b['id'] != main_batch]
			sub_batch_ids.sort()
			# retrieve data from batch
			for b in sub_batch_ids:
				r_ids = self.__bulkHelper.getQueryBatchResultIds(b, job)
				for r in r_ids:
					raw = self.__bulkHelper.getQueryBatchResults(b, r, job, raw=True)
					str_output = raw.read(decode_content=True).decode("utf-8", "replace")
					print( "Writing {} to file.".format(r) )
					# save to a file
					IOHelper.outputObjectToFile(object_name, str_output)
		except Exception as inst:
				print( inst )
				IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
		self.__bulkHelper.closeJob(job)
		print("$$$$$$ {} object downloaded!".format(object_name))

		# push to azure
		if Config.PUSH_TO_AZURE:
			self.__azureHelper.pushToAzure(object_name)


if __name__ == "__main__":
	chunkHelper = ChunkJobHelper.getInstance()
	print( chunkHelper )

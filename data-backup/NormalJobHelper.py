# -*- coding: utf-8 -*-

from BulkHelper import BulkHelper
from IOHelper import IOHelper
from AzureHelper import AzureHelper
from config import Config
import threading
import time

class NormalJobHelper:

	__instance = None
	norm_job_download_list = []
	norm_job_batch_list = []
	norm_job_object_dict = {}
	norm_result_list = []

	@staticmethod
	def getInstance():
		if NormalJobHelper.__instance == None:
			NormalJobHelper()
		return NormalJobHelper.__instance

	def __init__(self):
		if NormalJobHelper.__instance != None:
			raise Exception("NormalJobHelper class is a singleton!")
		else:
			NormalJobHelper.__instance = self
			self.__bulkHelper = BulkHelper.getInstance()

	def createJob(self, objectName, fieldList):
		print("###### Sending retrieve request: {} ".format(objectName))
		try:
			job_id, batch_id = self.__bulkHelper.createNormalBatch(objectName, fieldList)
			self.norm_job_batch_list.append({"job":job_id, "batch": batch_id})
			self.norm_job_download_list.append(job_id)
			self.norm_job_object_dict[job_id] = objectName
			IOHelper.appendToLog("norm_jobs.log",
					"\nobject: {}, job_id: {}, batch_id, {}".
					format(objectName, job_id, batch_id)
			)
		except Exception as inst:
			print( inst )
			IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))

	def startCheckNormalJobStatusThread(self):
		normalJobStatusCheckThread = NormalJobStatusCheckThread(self)
		normalJobStatusCheckThread.start()

	def startDownloadNormalBatchResultThread(self):
		downloadNormalBatchResultThread = DownloadNormalBatchResultThread(self)
		downloadNormalBatchResultThread.start()


class NormalJobStatusCheckThread (threading.Thread):

	def __init__(self, normalHelper):
		if not isinstance(normalHelper, NormalJobHelper):
			raise Exception("Variable normalHelper must be an instance of ChunkJobHelper!")
		self.__normalHelper = normalHelper
		self.__bulkHelper = BulkHelper.getInstance()
		threading.Thread.__init__(self)

	def run(self):
		print( "#### Starting Normal Job Status Checking Thread" )
		self._check_norm_batch_status()
		print("\n\n\n#### norm_result_list: {}".format(self.__normalHelper.norm_result_list))
		print( "#### Exiting Normal Job Status Checking Thread" )

	def _check_norm_batch_status(self):
		while len(self.__normalHelper.norm_job_batch_list) > 0:
			print("\n\n### Checking norm batch status...")
			print("norm_job_batch_list: {}".format(self.__normalHelper.norm_job_batch_list))
			for job_batch_dict in self.__normalHelper.norm_job_batch_list[:]:
				j_id = job_batch_dict['job']
				b_id = job_batch_dict['batch']
				try:
					b_status = self.__bulkHelper.getBatchStatus(b_id, j_id, True)
					b_state = b_status['state']
					object_name = self.__normalHelper.norm_job_object_dict[j_id]
					print( "\n# norm job: {} (object: {}), batch: {} status: {}".format(j_id, object_name, b_id, b_state) )
					print( "# full status result:" )
					print( b_status )
					if b_state == "Completed":
						result_id = self.__bulkHelper.getQueryBatchResultIds(b_id, j_id)[0]
						self.__normalHelper.norm_result_list.append({"job":j_id, "batch":b_id, "result":result_id})
						self.__normalHelper.norm_job_batch_list.remove(job_batch_dict)
					elif b_state == "Failed":
						IOHelper.appendToLog("extract_error.log",
								"\n\n# norm job: {} (object: {}), batch: {} status: {}".
								format(j_id, object_name, b_id, b_state)
						)
						self.__normalHelper.norm_job_batch_list.remove(job_batch_dict)
				except Exception as inst:
					print( inst )
					IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
				time.sleep(0.5)
			time.sleep(15)


class DownloadNormalBatchResultThread (threading.Thread):
	def __init__(self, normalHelper):
		if not isinstance(normalHelper, NormalJobHelper):
			raise Exception("Variable normalHelper must be an instance of ChunkJobHelper!")
		self.__normalHelper = normalHelper
		threading.Thread.__init__(self)
		self.__bulkHelper = BulkHelper.getInstance()
		self.__azureHelper = AzureHelper.getInstance()

	def run(self):
		print( "#### Starting Download Normal Batch Result Thread" )
		self._download_norm_batch_result()
		print( "#### Exiting Download Normal Batch Result Thread" )

	def _download_norm_batch_result(self):
		while len(self.__normalHelper.norm_job_download_list) > 0:
			print( "\n\n### Checking download tasks..." )
			print( "## norm_job_download_list (To Do): {}".format(self.__normalHelper.norm_job_download_list) )
			print( "## norm_result_list (Ready To Do): {}".format(self.__normalHelper.norm_result_list) )
			if len(self.__normalHelper.norm_result_list) > 0:
				for result_dict in self.__normalHelper.norm_result_list[:]:
					j_id = result_dict['job']
					b_id = result_dict['batch']
					r_id = result_dict['result']
					object_name = self.__normalHelper.norm_job_object_dict[j_id]
					if j_id in self.__normalHelper.norm_job_download_list:
						print( "# downloading job: {} - batch: {} - result: {}".format(j_id, b_id, r_id) )
						try:
							raw = self.__bulkHelper.getQueryBatchResults(b_id, r_id, j_id, raw=True)
							str_output = raw.read(decode_content=True).decode("utf-8", "replace")
							# save to a file
							# with open("output/{}.csv".format(object_name), "w") as text_file:
							# 	text_file.write(str_output)
							IOHelper.outputObjectToFile(object_name, str_output)
							self.__normalHelper.norm_job_download_list.remove(j_id)
							self.__normalHelper.norm_result_list.remove(result_dict)
							self.__bulkHelper.closeJob(j_id)
							print("$$$$$$ {} objected downloaded!".format(object_name))
						except Exception as inst:
							print( inst )
							IOHelper.appendToLog("api_error.log", "\n\n{}".format(inst))
						# push to azure
						if Config.PUSH_TO_AZURE:
							self.__azureHelper.pushToAzure(object_name)
					time.sleep(0.5)
			time.sleep(15)

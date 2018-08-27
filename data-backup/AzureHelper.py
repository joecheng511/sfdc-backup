# -*- coding: utf-8 -*-

import os
from azure.storage.blob import BlockBlobService
from config import Config
from IOHelper import IOHelper


class AzureHelper:

	__instance = None
	__block_blob_service = None

	@staticmethod
	def getInstance():
		if AzureHelper.__instance == None:
			AzureHelper()
		return AzureHelper.__instance

	def __init__(self):
		if AzureHelper.__instance != None:
			raise Exception("AzureHelper class is a singleton!")
		else:
			AzureHelper.__instance = self
			self.__block_blob_service = BlockBlobService(connection_string=Config.AZURE_CONNECTION_STRING)

	def pushToAzure(self, object_name):
		# push to azure
		if os.path.isfile("output/{}.csv".format(object_name)):
			try:
				self.__block_blob_service.create_blob_from_path(
					Config.AZURE_CONTAINER_NAME,
					Config.AZURE_FOLDER_NAME + "/{}.csv".format(object_name),
					"output/{}.csv".format(object_name)
				)
				print("^^^^^^ {} object pushed to Azure Storage Blob".format(object_name))
			except Exception as inst:
				print( inst )
				IOHelper.appendToLog("azure_error.log", "\n\n{}".format(inst))

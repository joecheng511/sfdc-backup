# -*- coding: utf-8 -*-

from SchemaHelper import SchemaHelper
from IOHelper import IOHelper
from BulkHelper import BulkHelper
from ChunkJobHelper import ChunkJobHelper
from NormalJobHelper import NormalJobHelper

# directory setup
# please note the data under log/ and output/ will be removed
IOHelper.init()

# retrieve the object schema
schemaHelper = SchemaHelper.getInstance()
object_fields_dict, object_chunkable_dict = schemaHelper.getObjectFieldDict()

# get the record count of each object retrieved
obj_record_count_dict = schemaHelper.getObjectRecordCount(object_fields_dict)

# send retrieve request to Salesforce Bulk API
chunkHelper = ChunkJobHelper.getInstance()
normalHelper = NormalJobHelper.getInstance()
for objectName,fieldList in object_fields_dict.items():
    if object_chunkable_dict[objectName]:
        chunkHelper.createJob(objectName, fieldList)
    else:
        normalHelper.createJob(objectName, fieldList)

# start job monitoring Threads
chunkHelper.startCheckChunkJobStatusThread()
normalHelper.startCheckNormalJobStatusThread()

# start downloading thread
normalHelper.startDownloadNormalBatchResultThread()

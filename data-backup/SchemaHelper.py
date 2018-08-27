# -*- coding: utf-8 -*-
"""
The class with the methods to get the schema via Salesforce REST API
"""

# See https://github.com/rbauction/sfdclib/
from sfdclib import SfdcSession, SfdcRestApi
from config import Config

class SchemaHelper:

    __instance = None
    __session = None
    __rest = None
    CHUNKABLE_OBJECT = ['Account', 'Campaign', 'CampaignMember', 'Case',
    	'Contact', 'Event', 'EventRelation', 'Lead',
    	'LoginHistory', 'Opportunity', 'Task', 'User']
    # skipped object list
    # See http://salesforcexytools.com/Salesforce/sfdc-dataloader-query-error.html
    NOT_BULK_API_SUPPORTED = [
    	'AcceptedEventRelation', 'CaseStatus', 'ContractStatus',
    	'KnowledgeArticle', 'KnowledgeArticleVersion',
    	'KnowledgeArticleVersionHistory', 'KnowledgeArticleViewStat',
    	'KnowledgeArticleVoteStat', 'LeadStatus', 'OpportunityStage',
    	'PartnerRole', 'RecentlyViewed', 'SolutionStatus',
    	'TaskPriority', 'UserRecordAccess', 'ContentFolderItem',
    	'DeclinedEventRelation', 'EventWhoRelation', 'TaskStatus',
    	'TaskWhoRelation', 'UndecidedEventRelation'
    ]
    MALFORMED_QUERY_OBJECT = [
    	'EntityParticle', 'SearchLayout', 'UserEntityAccess', 'RelationshipInfo',
    	'OwnerChangeOptionInfo', 'RelationshipDomain', 'PicklistValueInfo',
    	'IdeaComment', 'CollaborationGroupRecord', 'UserFieldAccess',
    	'ContentFolderMember', 'FieldDefinition', 'ContentDocumentLink',
    	'Vote', 'ContentHubItem', 'ColorDefinition', 'AppTabMember',
    	'IconDefinition'
    ]
    BINARY_OBJECT = [
    	'Attachment', 'ContentVersion', 'StaticResource', 'Document',
    	'EventLogFile', 'StaticResource', 'ContentNote', 'Note'
    ]
    EXTERNAL_OBJECT = [
    	'DataStatistics', 'FlexQueueItem', 'PlatformAction',
    	'ListViewChartInstance'
    ]
    UNKNOWN_EXCEPTION_OBJECT = [
    	'DatacloudAddress', 'EntityDefinition'
    ]

    @staticmethod
    def getInstance():
        if SchemaHelper.__instance == None:
            SchemaHelper()
        return SchemaHelper.__instance

    def __init__(self):
        if SchemaHelper.__instance != None:
            raise Exception("SchemaHelper class is a singleton!")
        else:
            SchemaHelper.__instance = self
            self.__session = SfdcSession(Config.USERNAME, Config.PASSWORD,
                Config.SECURITY_TOKEN, Config.IS_SANDBOX, Config.API_VERSION)
            self.__session.login()
            self.__rest = SfdcRestApi(self.__session)

    # Get objects and fields.
    # See https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_describeGlobal.htm
    # See https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_sobject_describe.htm
    def getObjectFieldDict(self):
        object_fields_dict = {}
        object_chunkable_dict = {}
        res_objects = self.__rest.get('/sobjects/')
        for obj in res_objects['sobjects']:
        	if len(Config.WHITE_LIST_OBJECT) > 0:
        		if obj['name'] not in Config.WHITE_LIST_OBJECT:
        			continue
        	if obj['queryable']:
        		print( obj['name'] )
        		if obj['name'] in self.NOT_BULK_API_SUPPORTED:
        			print( "### object: {} is not supported by the Bulk API".format(obj['name']) )
        		elif obj['name'] in self.MALFORMED_QUERY_OBJECT:
        			print( "### Condition is mandatory to narrowing columns to get object: {} ".format(obj['name']) )
        		elif obj['name'] in self.BINARY_OBJECT:
        			print( "### binary object: {} is not supported by the Bulk API".format(obj['name']) )
        		elif obj['name'] in self.EXTERNAL_OBJECT:
        			print( "### external object: {} is not supported by the Bulk API".format(obj['name']) )
        		elif obj['name'] in self.UNKNOWN_EXCEPTION_OBJECT:
        			print( "### object: {} is not supported by the Bulk API for unknown reason".format(obj['name']) )
        		elif obj['name'] in Config.SKIP_OBJECTS:
        			print( "### object: {} passed the process".format(obj['name']) )
        		elif obj['name'] == Config.BREAK_OBJECT:
        			print("### break by break object: {}. ".format(obj['name']))
        			break
        		else:
        			if obj['custom'] or obj['name'] in self.CHUNKABLE_OBJECT:
        				object_chunkable_dict[obj['name']] = True
        			else:
        				object_chunkable_dict[obj['name']] = False
        			field_list = []
        			compound_field_list = []
        			describe = self.__rest.get('/sobjects/{}/describe/'.format(obj['name']))
        			for field in describe['fields']:
        				field_list.append(field['name'])
        				compound_field_list.append(field['compoundFieldName'])
        			object_fields_dict[obj['name']] = [f for f in field_list if f not in compound_field_list]
        return object_fields_dict, object_chunkable_dict

    # Get Object record count
    # See https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_record_count.htm
    def getObjectRecordCount(self, object_fields_dict):
        obj_record_count_raw = self.__rest.get('/limits/recordCount?sObjects={}'.format(','.join(object_fields_dict.keys())))
        obj_record_count_dict = {}
        for item in obj_record_count_raw['sObjects']:
        	obj_record_count_dict[item['name']] = item['count']
        return obj_record_count_dict


if __name__ == "__main__":
    schemaHelper = SchemaHelper.getInstance()
    object_fields_dict, object_chunkable_dict = schemaHelper.getObjectFieldDict()
    print( object_fields_dict )
    print('\n\n')
    print( object_chunkable_dict )
    obj_record_count_dict = schemaHelper.getObjectRecordCount(object_fields_dict)
    print('\n\n')
    print(obj_record_count_dict)

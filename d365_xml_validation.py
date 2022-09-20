from aws_interaction import aws_interaction
from d365_interaction import d365_interaction_client
from bs4 import BeautifulSoup
from enum import Enum

class XML_file_storage(Enum):
    AWS_S3 = 0,
    AZURE_BLOB = 1

class d365_xml_validation_client():
    __logging__ = None
    __d365_interaction_client__ = None
    __aws_interaction_client__ = None
    
    def __init__(self, loggingMain):
        self.__logging__ = loggingMain
        self.__d365_interaction_client__ = d365_interaction_client()
        self.__aws_interaction_client__ = aws_interaction(loggingMain)


    def download_aws_payload(self, input_definition_group_id):
        aws_file_url = self.__aws_interaction_client__.find_aws_file_url(input_definition_group_id)
        payload_files = self.__aws_interaction_client__.download_payload_from_aws_s3_and_unzip(aws_file_url)

        return payload_files


    def download_azure_payload(self, input_definition_group_id):
        new_blob_url = self.__d365_interaction_client__.renew_sas_token_and_get_payload_url(input_definition_group_id)
        payload_files = self.__d365_interaction_client__.download_payload_from_blob_and_unzip(new_blob_url)

        return payload_files
    

    def validate_file_in_d365(self, input_definition_group_id, input_data_area_id, payload_files):
        try:
            for xmlFileName, xmlFileContent in payload_files.items():
                #self.__logging__.info("File: {0}; Content: {1}".format(xmlFileName, xmlFileContent))
                #self.__logging__.info(xmlFileName)
                
                bsXml = BeautifulSoup(xmlFileContent, "xml")
                
                match xmlFileName:
                    case "Manifest.xml":
                        #do nothing
                        pass
                        
                    case "PackageHeader.xml":
                        #do nothing
                        pass
                        
                    case "Customers V3.xml" | "customers.xml":
                        for xmlTag in bsXml.findAll("CUSTOMERACCOUNT"):
                            d365CustomerId = xmlTag.get_text()
                            
                            self.__logging__.info("{}:{}:Customer check:{}".format(input_definition_group_id, input_data_area_id, d365CustomerId))
                            
                            urlRequest = "/data/CustomersV3?$select=dataAreaId, CustomerAccount, CustomerGroupId&$filter=dataAreaId eq '{}' and CustomerAccount eq '{}'&cross-company=true".format(input_data_area_id, d365CustomerId)
                            if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                                self.__logging__.warning("{}:{}:Customer not exist:{}".format(input_definition_group_id, input_data_area_id, d365CustomerId))
                        
                    case "sales-order-headers.xml":
                        for xmlTag in bsXml.findAll("SALESORDERNUMBER"):
                            d365_sales_id = xmlTag.get_text()
                            
                            self.__logging__.info("{}:{}:Sales order check:{}".format(input_definition_group_id, input_data_area_id, d365_sales_id))
                            
                            urlRequest = "/data/SalesOrderHeadersV2?$select=dataAreaId, SalesOrderNumber&$filter=dataAreaId eq '{}' and SalesOrderNumber eq '{}'&cross-company=true".format(input_data_area_id, d365_sales_id)
                            if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                                self.__logging__.warning("{}:{}:Sales order not exist:{}".format(input_definition_group_id, input_data_area_id, d365_sales_id))
                        
                    case "sales-order-lines.xml":
                        for xmlTag in bsXml.findAll("INVENTORYLOTID"):
                            d365_inventory_lot_id = xmlTag.get_text()
                            
                            self.__logging__.info("{}:{}:Sales order line check:{}".format(input_definition_group_id, input_data_area_id, d365_inventory_lot_id))
                            
                            urlRequest = "/data/SalesOrderLines?$select=dataAreaId, InventoryLotId&$filter=dataAreaId eq '{}' and InventoryLotId eq '{}'&cross-company=true".format(input_data_area_id, d365_inventory_lot_id)
                            if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                                self.__logging__.warning("{}:{}:Sales order line not exist:{}".format(input_definition_group_id, input_data_area_id, d365_inventory_lot_id))
            
                    case "general-journal.xml":
                        xmlTag1 = bsXml.find("DOCUMENT")
                        xmlTag2 = bsXml.find("LINENUMBER")

                        while xmlTag1:
                            d365Document = xmlTag1.get_text()
                            d365LineNumber = xmlTag2.get_text()
                            
                            self.__logging__.info("{}:{}:General journal line check:{}:{}".format(input_definition_group_id, input_data_area_id, d365Document, d365LineNumber))
                            
                            urlRequest = "/data/LedgerJournalLines?$filter=dataAreaId eq '{}' and Document eq '{}' and LineNumber eq {}&cross-company=true".format(input_data_area_id, d365Document, d365LineNumber)
                            if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                                self.__logging__.warning("{}:{}:General journal line not exist:{}:{}".format(input_definition_group_id, input_data_area_id, d365Document, d365LineNumber))
                        
                            xmlTag1 = xmlTag1.findNext("DOCUMENT")
                            xmlTag2 = xmlTag2.findNext("LINENUMBER")
                            
                    case _:
                        self.__logging__.warning("{}:{}:Unknown file:{}".format(input_definition_group_id, input_data_area_id, xmlFileName))  
        except:
            self.__logging__.error('{}:{}:Failed to download and parse'.format(input_definition_group_id, input_data_area_id))
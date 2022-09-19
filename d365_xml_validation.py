from d365_interaction import d365_interaction_client
from bs4 import BeautifulSoup


class d365_xml_validation_client():
    __logging__ = None
    __d365_interaction_client__ = None
    
    
    def __init__(self, loggingMain):
        self.__logging__ = loggingMain
        self.__d365_interaction_client__ = d365_interaction_client()
    
    
    def validate_file_in_d365(self, inputDefinitionGroupId, inputDataAreaId):
        newBlobUrl = self.__d365_interaction_client__.renew_sas_token_and_get_payload_url(inputDefinitionGroupId)
        payloadFiles = self.__d365_interaction_client__.download_payload_from_blob_and_unzip(newBlobUrl)

        for xmlFileName, xmlFileContent in payloadFiles.items():
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
                        
                        self.__logging__.info("{}:{}:Customer check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365CustomerId))
                        
                        urlRequest = "/data/CustomersV3?$select=dataAreaId, CustomerAccount, CustomerGroupId&$filter=dataAreaId eq '{}' and CustomerAccount eq '{}'&cross-company=true".format(inputDataAreaId, d365CustomerId)
                        if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                            self.__logging__.warning("{}:{}:Customer not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365CustomerId))
                    
                case "sales-order-headers.xml":
                    for xmlTag in bsXml.findAll("SALESORDERNUMBER"):
                        d365SalesId = xmlTag.get_text()
                        
                        self.__logging__.info("{}:{}:Sales order check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365SalesId))
                        
                        urlRequest = "/data/SalesOrderHeadersV2?$select=dataAreaId, SalesOrderNumber&$filter=dataAreaId eq '{}' and SalesOrderNumber eq '{}'&cross-company=true".format(inputDataAreaId, d365SalesId)
                        if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                            self.__logging__.warning("{}:{}:Sales order not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365SalesId))
                    
                case "sales-order-lines.xml":
                    for xmlTag in bsXml.findAll("INVENTORYLOTID"):
                        d365InventoryLotId = xmlTag.get_text()
                        
                        self.__logging__.info("{}:{}:Sales order line check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365InventoryLotId))
                        
                        urlRequest = "/data/SalesOrderLines?$select=dataAreaId, InventoryLotId&$filter=dataAreaId eq '{}' and InventoryLotId eq '{}'&cross-company=true".format(inputDataAreaId, d365InventoryLotId)
                        if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                            self.__logging__.warning("{}:{}:Sales order line not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365InventoryLotId))
        
                case "general-journal.xml":
                    xmlTag1 = bsXml.find("DOCUMENT")
                    xmlTag2 = bsXml.find("LINENUMBER")

                    while xmlTag1:
                        d365Document = xmlTag1.get_text()
                        d365LineNumber = xmlTag2.get_text()
                        
                        self.__logging__.info("{}:{}:General journal line check:{}:{}".format(inputDefinitionGroupId, inputDataAreaId, d365Document, d365LineNumber))
                        
                        urlRequest = "/data/LedgerJournalLines?$filter=dataAreaId eq '{}' and Document eq '{}' and LineNumber eq {}&cross-company=true".format(inputDataAreaId, d365Document, d365LineNumber)
                        if not self.__d365_interaction_client__.odata_check_exist(urlRequest):
                            self.__logging__.warning("{}:{}:General journal line not exist:{}:{}".format(inputDefinitionGroupId, inputDataAreaId, d365Document, d365LineNumber))
                    
                        xmlTag1 = xmlTag1.findNext("DOCUMENT")
                        xmlTag2 = xmlTag2.findNext("LINENUMBER")
                        
                case _:
                    self.__logging__.warning("{}:{}:Unknown file:{}".format(inputDefinitionGroupId, inputDataAreaId, xmlFileName))  

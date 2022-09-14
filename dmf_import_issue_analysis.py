import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import csv
import logging
from tqdm import tqdm

from d365_interaction import D365Interaction

load_dotenv()

logging.basicConfig(
    filename=os.getenv("inputCsvDatasetFileName") + ".log",
    encoding="utf-8",
    level=logging.INFO,
    filemode="a")

d365Interaction = D365Interaction(logging)

def validate_file_in_d365(inputDefinitionGroupId, inputDataAreaId):
    newBlobUrl = d365Interaction.renew_sas_token_and_get_payload_url(inputDefinitionGroupId)
    payloadFiles = d365Interaction.download_payload_from_blob_and_unzip(newBlobUrl)

    for xmlFileName, xmlFileContent in payloadFiles.items():
        #logging.info("File: {0}; Content: {1}".format(xmlFileName, xmlFileContent))
        #logging.info(xmlFileName)
        
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
                    
                    logging.info("{}:{}:Customer check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365CustomerId))
                    
                    urlRequest = "/data/CustomersV3?$select=dataAreaId, CustomerAccount, CustomerGroupId&$filter=dataAreaId eq '{}' and CustomerAccount eq '{}'&cross-company=true".format(inputDataAreaId, d365CustomerId)
                    if not d365Interaction.odata_check_exist(urlRequest):
                        logging.warning("{}:{}:Customer not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365CustomerId))
                
            case "sales-order-headers.xml":
                for xmlTag in bsXml.findAll("SALESORDERNUMBER"):
                    d365SalesId = xmlTag.get_text()
                    
                    logging.info("{}:{}:Sales order check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365SalesId))
                    
                    urlRequest = "/data/SalesOrderHeadersV2?$select=dataAreaId, SalesOrderNumber&$filter=dataAreaId eq '{}' and SalesOrderNumber eq '{}'&cross-company=true".format(inputDataAreaId, d365SalesId)
                    if not d365Interaction.odata_check_exist(urlRequest):
                        logging.warning("{}:{}:Sales order not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365SalesId))
                
            case "sales-order-lines.xml":
                for xmlTag in bsXml.findAll("INVENTORYLOTID"):
                    d365InventoryLotId = xmlTag.get_text()
                    
                    logging.info("{}:{}:Sales order line check:{}".format(inputDefinitionGroupId, inputDataAreaId, d365InventoryLotId))
                    
                    urlRequest = "/data/SalesOrderLines?$select=dataAreaId, InventoryLotId&$filter=dataAreaId eq '{}' and InventoryLotId eq '{}'&cross-company=true".format(inputDataAreaId, d365InventoryLotId)
                    if not d365Interaction.odata_check_exist(urlRequest):
                        logging.warning("{}:{}:Sales order line not exist:{}".format(inputDefinitionGroupId, inputDataAreaId, d365InventoryLotId))
    
            case "general-journal.xml":
                xmlTag1 = bsXml.find("DOCUMENT")
                xmlTag2 = bsXml.find("LINENUMBER")

                while xmlTag1:
                    d365Document = xmlTag1.get_text()
                    d365LineNumber = xmlTag2.get_text()
                    
                    logging.info("{}:{}:General journal line check:{}:{}".format(inputDefinitionGroupId, inputDataAreaId, d365Document, d365LineNumber))
                    
                    urlRequest = "/data/LedgerJournalLines?$filter=dataAreaId eq '{}' and Document eq '{}' and LineNumber eq {}&cross-company=true".format(inputDataAreaId, d365Document, d365LineNumber)
                    if not d365Interaction.odata_check_exist(urlRequest):
                        logging.warning("{}:{}:General journal line not exist:{}:{}".format(inputDefinitionGroupId, inputDataAreaId, d365Document, d365LineNumber))
                
                    xmlTag1 = xmlTag1.findNext("DOCUMENT")
                    xmlTag2 = xmlTag2.findNext("LINENUMBER")
                    
            case _:
                logging.warning("{}:{}:Unknown file:{}".format(inputDefinitionGroupId, inputDataAreaId, xmlFileName))  

with open(os.getenv("inputCsvDatasetFileName"), mode='r') as inputCsvFile:
    rowNo = 0
    totalRows = sum(1 for _ in inputCsvFile)
    inputCsvFile.seek(0)
    
    inputDataSet = csv.DictReader(inputCsvFile, delimiter=";")
    for inputDataRow in tqdm(inputDataSet, total=totalRows):
        rowNo += 1
        
        if rowNo == 1: #Header
            continue
        
        inputDefinitionGroupId = "{0}-{1}.zip".format(inputDataRow["ENTITY"], inputDataRow["BATCH_ID"])
        inputDataAreaId = inputDataRow["LEGAL_ENTITY"]
        
        validate_file_in_d365(inputDefinitionGroupId, inputDataAreaId)
        
        #if rowNo == 5:
        #    break
        
    print("Done. {} rows processed".format(totalRows))
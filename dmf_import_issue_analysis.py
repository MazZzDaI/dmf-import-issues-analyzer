import os
import requests
import json
import zipfile 
import io
from io import StringIO, BytesIO
from dotenv import load_dotenv

load_dotenv()

baseUrl = os.getenv("baseUrl")

class DmfInteraction():       
    xmlManifest = ""
    xmlPackageHeader = ""
    xmlContentFilesDict = [""]
     
    def __auth_to_d365(self):
        aadTenantId = os.getenv("aadTenantId")
        aadClientId = os.getenv("aadClientId")
        aadSecretKey = os.getenv("aadSecretKey")
        
        url = "https://login.microsoftonline.com/" + aadTenantId + "/oauth2/token"
        payload={"grant_type": "client_credentials",
        "client_id": aadClientId,
        "client_secret": aadSecretKey,
        "resource": baseUrl}

        response = requests.request("POST", url, data=payload)
        jsonResponse = json.loads(response.text)
        accessToken = jsonResponse["access_token"]
        #print(accessToken)
        return accessToken

    def renew_sas_token_and_get_payload_url(self, payloadFileName):
        url = baseUrl + "/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl"

        payload = json.dumps({
            "uniqueFileName": payloadFileName
        })
        
        headers = {
            "Authorization": "Bearer " + self.__auth_to_d365(),
            "Content-Type": "application/json"
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        jsonResponse = json.loads(response.text)
        jsonBlobValue = json.loads(jsonResponse["value"])
        blobUrl = jsonBlobValue["BlobUrl"]
        #print(blobUrl)
        return blobUrl
    
    def download_payload_from_blob_and_unzip(self, payloadUrl):
        u = requests.get(payloadUrl)
        f = io.BytesIO() 
        f.write(u.content)
        z = zipfile.ZipFile(f)
        
        #for fileNo in z.namelist():
        
        z.extractall()
        payloadFiles = {i: {z.read(i), z.open(i)} for i in z.namelist()}
        
        return payloadFiles
        
        
dmfInteraction = DmfInteraction()
newBlobUrl = dmfInteraction.renew_sas_token_and_get_payload_url("ODu-Customers-Test-Import.zip")
payloadFiles = dmfInteraction.download_payload_from_blob_and_unzip(newBlobUrl)

for xmlFile in payloadFiles:
    print(xmlFile[0])
    print(xmlFile[1])
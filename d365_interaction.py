import logging
import os
from tkinter.messagebox import RETRY
import requests
import json
import zipfile 
import io
from retry import retry

baseUrl = os.getenv("baseUrl")

class D365Interaction():
    __accessToken__ = ""
    __logging__ = None
    
    def __init__(self, loggingMain):
        self.__accessToken__ = self.__auth_to_d365()
        self.__logging__ = loggingMain
    
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
        #print("DEBUG: {0}".format(accessToken))
        return accessToken

    def renew_sas_token_and_get_payload_url(self, payloadFileName):
        url = baseUrl + "/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl"

        payload = json.dumps({
            "uniqueFileName": payloadFileName
        })
        
        headers = {
            "Authorization": "Bearer " + self.__accessToken__,
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
        
        payloadFiles = {i: z.read(i) for i in z.namelist()}
        
        return payloadFiles
    
    @retry(delay=1, tries=5, jitter=1)
    def odata_check_exist(self, urlRequest):
        headers = {
            'Authorization': 'Bearer ' + self.__accessToken__
        }

        recordsNumber = 0
        response = requests.request("GET", baseUrl + urlRequest, headers=headers)
        jsonResponse = json.loads(response.text)
        recordsNumber = len(jsonResponse["value"])
    
        return recordsNumber != 0
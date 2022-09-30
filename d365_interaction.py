import logging
import os
from time import sleep
import requests
import json
import zipfile 
import io
from retry import retry

base_url = os.getenv('base_url')

class D365_interaction_client():
    __access_token__ = ''

    
    def __init__(self):
        self.__access_token__ = self.__auth_to_d365__()

    
    def __auth_to_d365__(self):
        aad_tenant_id = os.getenv('aad_tenant_id')
        aad_client_id = os.getenv('aad_client_id')
        aad_secret_key = os.getenv('aad_secret_key')
        
        url = 'https://login.microsoftonline.com/' + aad_tenant_id + '/oauth2/token'
        payload={'grant_type': 'client_credentials',
            'client_id': aad_client_id,
            'client_secret': aad_secret_key,
            'resource': base_url}

        response = requests.request('POST', url, data=payload)
        json_response = json.loads(response.text)
        access_token = json_response['access_token']
        return access_token


    def renew_sas_token_and_get_payload_url(self, payload_file_name):
        url = base_url + '/data/DataManagementDefinitionGroups/Microsoft.Dynamics.DataEntities.GetAzureWriteUrl'

        payload = json.dumps({
            'uniqueFileName': payload_file_name
        })
        
        headers = {
            'Authorization': 'Bearer ' + self.__access_token__,
            'Content-Type': 'application/json'
        }

        response = requests.request('POST', url, headers=headers, data=payload)
        json_response = json.loads(response.text)
        json_blob_value = json.loads(json_response['value'])
        blobUrl = json_blob_value['BlobUrl']
        return blobUrl
    
    
    def download_payload_from_blob_and_unzip(self, payload_url):
        u = requests.get(payload_url)
        if u.status_code == 404:
            return None

        f = io.BytesIO()
        f.write(u.content)
        z = zipfile.ZipFile(f)
        
        payload_files = {i: z.read(i) for i in z.namelist()}
        
        return payload_files
    
    
    @retry(delay=1, tries=5, jitter=1)
    def odata_check_exist(self, url_request):
        records_number = self.odata_records_count(url_request)
        return records_number != 0 
    
    
    @retry(delay=1, tries=5, jitter=1)
    def odata_records_count(self, url_request):
        json_response = self.odata_get(url_request)
        records_number = len(json_response['value'])
        return records_number
    
    
    @retry(delay=1, tries=5, jitter=1)
    def odata_get(self, url_request):
        headers = {
            'Authorization': 'Bearer ' + self.__access_token__
        }

        records_number = 0
        response = requests.request('GET', base_url + url_request, headers=headers)
        if response.status_code == 429:
            sleep(float(response.headers.get('Retry-After')))
            response = requests.request('GET', base_url + url_request, headers=headers)
            
        json_response = json.loads(response.text)
        return json_response
import os
from dotenv import load_dotenv
import logging
import time
import csv
from tqdm import tqdm
from d365_xml_validation import d365_xml_validation_client

def main():
    load_dotenv()

    logging.basicConfig(
        filename=os.getenv("input_csv_dataset_filename") + ".log",
        encoding="utf-8",
        level=logging.INFO,
        filemode="w")
        
    d365_xml_validator = d365_xml_validation_client(logging)
    
    started_at = time.monotonic()
    total_packages = 0
    
    with open(os.getenv("input_csv_dataset_filename"), mode='r') as inputCsvFile:    
        rowNo = 0
        total_packages = sum(1 for _ in inputCsvFile)
        
        inputCsvFile.seek(0)
        inputDataSet = csv.DictReader(inputCsvFile, delimiter=";")
        
        for inputDataRow in tqdm(inputDataSet, total=total_packages):
            rowNo += 1
            
            if rowNo == 1: #Header
                continue
            
            inputDefinitionGroupId = "{0}-{1}.zip".format(inputDataRow["ENTITY"], inputDataRow["BATCH_ID"])
            inputDataAreaId = inputDataRow["LEGAL_ENTITY"]
            
            d365_xml_validator.validate_file_in_d365(inputDefinitionGroupId, inputDataAreaId)

    total_execution_time = time.monotonic() - started_at

    print('====')
    #print(f'{os.getenv("async_workers_count")} worker(s) completed {tasks_count} task(s) in parallel for {total_execution_time / 60:.2f} minute(s)')
    print(f'{total_packages} package(s) processed in {total_execution_time / 60:.2f} minute(s)')
    
main()
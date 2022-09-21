import os
from dotenv import load_dotenv
import logging
import time
import csv
from tqdm import tqdm
from d365_xml_validation import d365_xml_validation_client
import asyncio


async def work(args, d365_xml_validator, logging):
    input_definition_group_id, inputDataAreaId = args
    try:
        payload_files = await d365_xml_validator.download_aws_payload(input_definition_group_id)

        #if payload_files:
        #    await d365_xml_validator.validate_file_in_d365(input_definition_group_id, inputDataAreaId, payload_files)
    except:
        logging.error('{}:{}:No file found in AWS'.format(input_definition_group_id, ''))

async def worker(worker_id, queue, pbar, d365_xml_validator, logging):
    while True:
        args = await queue.get()

        await work(args, d365_xml_validator, logging)        
        logging.info(worker_id)

        queue.task_done()
        pbar.update(1)


async def main():
    load_dotenv()
    logging.basicConfig(
        filename=os.path.join('log', os.getenv("input_csv_dataset_filename") + '.log'),
        encoding="utf-8",
        level=logging.INFO,
        filemode="w")
        
    d365_xml_validator = d365_xml_validation_client(logging)
    
    queue = asyncio.Queue()
    started_at = time.monotonic()
    total_packages = 0
    
    with open(os.path.join('input', os.getenv("input_csv_dataset_filename")), mode='r') as inputCsvFile:    
        rowNo = 0
        total_packages = sum(1 for _ in inputCsvFile)
        
        inputCsvFile.seek(0)
        inputDataSet = csv.DictReader(inputCsvFile, delimiter=os.getenv('csv_dataset_separator'))
        
        for inputDataRow in tqdm(inputDataSet, total=total_packages):
            rowNo += 1
            
            #inputDefinitionGroupId = "{0}-{1}.zip".format(inputDataRow["ENTITY"], inputDataRow["BATCH_ID"])
            #payload_files = d365_xml_validator.download_azure_payload(inputDefinitionGroupId)
            #if payload_files:
            #    d365_xml_validator.validate_file_in_d365(inputDefinitionGroupId, inputDataAreaId, payload_files)

            inputDefinitionGroupId = "{0}".format(inputDataRow["BATCH_ID"])
            inputDataAreaId = inputDataRow["LEGAL_ENTITY"]

            queue.put_nowait([inputDefinitionGroupId, inputDataAreaId])


    pbar = tqdm(total=rowNo)

    tasks = []
    for i in range(int(os.getenv("async_workers_count"))):
        task = asyncio.create_task(worker(i, queue, pbar, d365_xml_validator, logging))
        tasks.append(task)

    started_at = time.monotonic()
    await queue.join()
    total_execution_time = time.monotonic() - started_at

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    print('====')
    #print(f'{os.getenv("async_workers_count")} worker(s) completed {tasks_count} task(s) in parallel for {total_execution_time / 60:.2f} minute(s)')
    print(f'{total_packages} package(s) processed in {total_execution_time / 60:.2f} minute(s)')


asyncio.run(main())
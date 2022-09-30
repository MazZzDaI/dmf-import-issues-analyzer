import os
from dotenv import load_dotenv
import logging
import time
import csv
from tqdm import tqdm
import asyncio
from d365_interaction import D365_interaction_client

load_dotenv()
logging.basicConfig(
    filename=os.path.join('log', os.getenv("input_csv_dataset_filename") + '.log'),
    format='%(levelname)s:%(message)s',
    encoding="utf-8",
    level=logging.INFO,
    filemode="w")        

d365_interaction_client = D365_interaction_client()

##
async def worker(worker_id, queue, pbar):
    while True:
        args = await queue.get()
        input_data_area_id, d365_gl_journal_batch_number, d365_gl_document, d3656_gl_line_number = args
        urlRequest = "/data/LedgerJournalLines?$filter=dataAreaId eq '{}' and Document eq '{}' and LineNumber eq {}&cross-company=true".format(input_data_area_id, d365_gl_document, d3656_gl_line_number)

        try:
            loop = asyncio.get_event_loop()
            future1 = loop.run_in_executor(None, d365_interaction_client.odata_records_count, urlRequest)
            
            gl_records_number = await future1
            logging.info(f"{worker_id};{input_data_area_id};{d365_gl_journal_batch_number};{d365_gl_document};{d3656_gl_line_number};{gl_records_number}")   
        except Exception as e:
            logging.warning(f"{worker_id};{input_data_area_id};{d365_gl_journal_batch_number};{d365_gl_document};{d3656_gl_line_number};{gl_records_number};{e}")   
            
        queue.task_done()
        pbar.update(1)

##
async def main():
    started_at = time.monotonic()

    '''
    log_header_line = ''
    urlRequest = "/data/LedgerJournalLines?$top=1&cross-company=true"
    gl_record_json_response = d365_interaction_client.odata_get(urlRequest)
    for key in gl_record_json_response['value'][0].keys():
        log_header_line += f'{"," if log_header_line != "" else ""}{key}'
    logging.info(log_header_line)
    '''

    queue = asyncio.Queue()

    with open(os.path.join('input', os.getenv("input_csv_dataset_filename")), mode='r', encoding='utf-8-sig') as inputCsvFile:    
        rowNo = 0
        total_file_records = sum(1 for _ in inputCsvFile)
        
        inputCsvFile.seek(0)
        inputDataSet = csv.DictReader(inputCsvFile, delimiter=os.getenv('csv_dataset_separator'))
        logging.info("worker_id;data_area_id;gl_document;gl_line_number;gl_records_number")   

        for inputDataRow in inputDataSet:
            rowNo += 1

            input_data_area_id = inputDataRow["LegalEntity"]
            d365_gl_document = inputDataRow["Document"]
            d3656_gl_line_number = inputDataRow["LineNumber"].replace(',00', '')
            d365_gl_journal_batch_number = inputDataRow["JournalBatchNumber"]
            
            queue.put_nowait([input_data_area_id, d365_gl_journal_batch_number, d365_gl_document, d3656_gl_line_number])


    pbar = tqdm(total=total_file_records)

    tasks = []
    for i in range(int(os.getenv("async_workers_count"))):
        task = asyncio.create_task(worker(i, queue, pbar))
        tasks.append(task)

    await queue.join()
    total_execution_time = time.monotonic() - started_at

    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)

    print('====')
    #print(f'{os.getenv("async_workers_count")} worker(s) completed {tasks_count} task(s) in parallel for {total_execution_time / 60:.2f} minute(s)')
    print(f'{total_file_records} package(s) processed in {total_execution_time / 60:.2f} minute(s)')


asyncio.run(main())
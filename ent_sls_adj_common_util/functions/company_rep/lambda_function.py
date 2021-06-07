import traceback
import logging
from modify_record import modify_handler
from insert_record import insert_handler
from cross_account_session import get_cross_account_session
from config import(
    
    ARN
    
    )

#Add logging formatter
formatter=logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    
# Creaate a logger and setting the logging level
logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class NotAgencyProducerRecord(Exception):
    def __init__(self,message):
        self.message=message


def lambda_handler(event, context):
    
    # TODO implement
    try:
        cross_accnt_session = None
        for record in event['Records']:
            pk = record['dynamodb']['Keys']['pk']['N']
            sk = record['dynamodb']['Keys']['sk']['S']
            
            if not sk.startswith('agency_producer#producernbr#'):
                continue
                
            cross_accnt_session = get_cross_account_session(ARN)
            logger.info("cross account session created")
            print(event)  
            if record['eventName'] == 'INSERT':
                logger.info("New agency Created .. Begin company rep data insertion")
                insert_handler(record,cross_accnt_session,logger)
                
            elif record['eventName'] == 'MODIFY':
                co_rep_map_new = record['dynamodb']['NewImage']['co_rep_map']['L']
                agncy_cd_new = record['dynamodb']['NewImage']['agncy_cd']['S']
                co_rep_map_old = record['dynamodb']['OldImage']['co_rep_map']['L']
                agncy_cd_old = record['dynamodb']['OldImage']['agncy_cd']['S']
                insert_handler(record,cross_accnt_session,logger)
                if co_rep_map_new != co_rep_map_old or agncy_cd_new != agncy_cd_old:
                    logger.info("Begin company rep data modification")
                    modify_handler(record,cross_accnt_session,logger)
                else:
                    logger.info("No data to modify")
                    
            
    except Exception as ex:
        log_message = f"Exception occurred while updating records to dynamodb {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
import sys
import os
path = os.path.abspath(__file__)
dir_path = os.path.dirname(path)
sys.path.insert(0,dir_path)
import awswrangler.exceptions as exceptions
from botocore.exceptions import ClientError
import awswrangler as wr
from boto3.dynamodb.conditions import Key, Attr
import boto3
from datetime import datetime
import traceback
from datetime import timedelta
from pytz import timezone
import time
from config import(
    
    TABLE_NAME
    
    )
def get_today_loc_dt():
    eastern = timezone('US/Eastern')
    today_loc_dt = datetime.now(tz=eastern)
    return today_loc_dt

dynamodb = boto3.resource('dynamodb')
co_rep_data_table  = dynamodb.Table(TABLE_NAME)

def execute_query_from_athena(logger,query_str, cross_account_session, database_name):
    df = None
    for i in range(3):
        try:
            df =  wr.athena.read_sql_query(sql = query_str, database=database_name,
                ctas_approach=False, boto3_session = cross_account_session)
        except exceptions.QueryFailed as e:
            logger.error('Attempt {} of Athena Query execution has failed. '.format(i + 1))
            time.sleep(10)

        except exceptions.QueryCancelled as e:
            logger.error('Attempt {} of Athena Query execution has been Cancelled. '.format(i + 1))
            time.sleep(10)
        except ClientError as e:
            logger.error('Client Side Error. Most common cause: Invalid Query')
            break
        else:
            break
    return df

def valid_employee(emp_num ,cross_accnt_session,logger):
    try:
        
        dl_data = None
        
        query = ''' select * from uatrun_processed_sabir.employee 
        where batch = (select max(batch) from uatrun_processed_sabir.employee) and employeeno={}; '''.format(emp_num)
        dl_data = execute_query_from_athena(logger,query_str = query,
                                                  cross_account_session=
                                                  cross_accnt_session, database_name="default")
                                                  
        dl_data = dl_data.fillna("")
        
        if len(dl_data)>0:
            return {'emp_no':dl_data['employeeno'].iloc[0],'op_id':dl_data['operatorid'].iloc[0]}
        else:
            return False
    
    except Exception as ex:
        log_message = f"Exception occurred in valid_employee {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
        
        
def get_rep_cd_list(table,lastname_3_char,logger):
    try:
        rep_data_list =[]
        scan_kwargs = {
        'FilterExpression': Attr('co_rep_cd').begins_with(lastname_3_char),
        'ProjectionExpression': "co_rep_cd",
        
        }
        done = False
        start_key = None
        while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = table.scan(**scan_kwargs)
                item = response.get('Items', [])
                if len(item)>0:
                    rep_data_list.extend(item)
                
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
                
        return rep_data_list
        
    except Exception as ex:
        log_message = f"Exception occurred in get_rep_cd_list {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
        
        
def get_com_rep_code(table,emp_type,emp_num,logger):
    
    try:
        rep_data =[]
        scan_kwargs = {
    'FilterExpression': Attr('sk').begins_with('emptyp#{}'.format(emp_type)) & Attr('emp_id').eq(emp_num),
    'ProjectionExpression': "co_rep_cd,pk,sk",
    
    }

        done = False
        start_key = None
        while not done:
            
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            item = response.get('Items', [])
            if len(item)>0:
                rep_data.extend(item)
                break
            start_key = response.get('LastEvaluatedKey', None)
            
            done = start_key is None
            
        return rep_data
        
    except Exception as ex:
        log_message = f"Exception occurred in get_com_rep_code {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
        

def get_emp_type_from_table(table,role_cd,logger):
    
    try:
        response = table.query(
            ProjectionExpression = "emp_type_cd",
            KeyConditionExpression= Key('pk').eq("role#{}".format(role_cd))
        )
        items = response['Items']
        logger.info(f"role_code:{role_cd},{items}")
        return items
    except Exception as ex:
        log_message = f"Exception occurred in get_emp_type_from_table {ex} \n {traceback.format_exc()}"
        logger.error(log_message)        
        
def get_emp_type(table,role_code,logger):
    
    emp_type = get_emp_type_from_table(table,role_code,logger)[0]
    return emp_type['emp_type_cd']
    
def is_rows_present(table,agncy_cd,emp_type,market_id,logger):
    
    try:
        response = table.query(
        KeyConditionExpression=Key('pk').eq("agcy#{}".format(agncy_cd)) & Key('sk').eq('emptyp#{}#markt#{}'.format(emp_type,market_id))
    )
        if(len(response['Items'])) > 0:
            return True
        else:
            return False
            
        '''rep_data =[]
        scan_kwargs = {
    'FilterExpression': Key('pk').eq("agcy#{}".format(agncy_cd)) & Attr('sk').eq('emptyp#{}markt#{}'.format(emp_type,market_id)),
    'ProjectionExpression': "sk",
    
    }

        done = False
        start_key = None
        while not done:
            
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            item = response.get('Items', [])
            if len(item)>0:
                rep_data.extend(item)
                break
            start_key = response.get('LastEvaluatedKey', None)
            
            done = start_key is None
            
        if len(rep_data)>0:
            return True
        else:
            return False'''
        
    except Exception as ex:
        log_message = f"Exception occurred in is_rows_present {ex} \n {traceback.format_exc()}"
        logger.error(log_message)  

def modify_agncy_emp_typ(table,agncy_cd,emp_type,market_id,deleted_flag,co_rep_cd,emp_num,logger):
    try:
        exp_attr={
                ':co_rep_cd':co_rep_cd,
                ':eff_dt':get_today_loc_dt().strftime('%Y-%m-%d'),
                ':updt_dttm' :get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S'),
                ':emp_id':emp_num,
                ':gs1pk':'empno#{}'.format(emp_num),
                ':gs2pk':'rep#{}'.format(co_rep_cd),
                ':deleted_flag' : deleted_flag ,
            }
        logger.info(f"expression attributes : {exp_attr}")
        response = table.update_item(
            Key={
                'pk': 'agcy#{}'.format(agncy_cd),
                'sk': 'emptyp#{}#markt#{}'.format(emp_type,market_id)
            },
            UpdateExpression="set co_rep_cd=:co_rep_cd,eff_dt=:eff_dt,updt_dttm=:updt_dttm,emp_id=:emp_id,gs1pk=:gs1pk,gs2pk=:gs2pk,deleted_flag=:deleted_flag",
            ExpressionAttributeValues={
                ':co_rep_cd':co_rep_cd,
                ':eff_dt':get_today_loc_dt().strftime('%Y-%m-%d'),
                ':updt_dttm':get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S'),
                ':emp_id': emp_num,
                ':gs1pk':'empno#{}'.format(emp_num),
                ':gs2pk':'rep#{}'.format(co_rep_cd),
                ':deleted_flag' : deleted_flag ,
            },
            ReturnValues="UPDATED_NEW"
        )
        return response
    
    except Exception as ex:
        log_message = f"Exception occurred in modify_agncy_emp_typ {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
        
def delete_market_0(table,agncy_cd,emp_type,logger):
    try:
        response = table.delete_item(
            Key={
                'pk': 'agcy#{}'.format(agncy_cd),
                'sk': 'emptyp#{}#markt#{}'.format(emp_type,'0')
            }
        )
        logger.info (f'deleted for market_id = 0 , agncy_cd : {agncy_cd}, emp_type : {emp_type}' )
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.error(e.response['Error']['Message'])
        else:
            raise
    else:
        return response

def employee_already_for_emptyp_marketid(table,agncy_cd,emp_type,market_id,emp_num,logger):
    try:
        response = table.query(
        IndexName='gs1pk-gs1sk-index',
        KeyConditionExpression=Key('gs1pk').eq("empno#{}".format(emp_num)) & Key('gs1sk').eq('emptyp#{}#agncy#{}#markt#{}'.format(emp_type,agncy_cd,market_id))
    )
        if(len(response['Items'])) > 0:
            return True
        else:
            return False
            
    except Exception as ex:
        log_message = f"Exception occurred in is_rows_present {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
        
def add_co_rep_data(rep_detail,new_image,cross_accnt_session,logger):
    try:
        logger.info(f"Beigin to add employee in co_rep_dat DDB")
        co_rep_cd = ''
        emp_num = str(int(rep_detail['M']['co_rep_cd']['S']))
        valid_emp = valid_employee(emp_num,cross_accnt_session,logger)
        if valid_emp != False:
            logger.info(f"{emp_num} :Valid employee")
            
            role_code = rep_detail['M']['org_role_cd']['S']
            market_id = rep_detail['M']['lob_id']['S']
            co_rep_desc = rep_detail['M']['lob_nm']['S']
            emp_type = get_emp_type(co_rep_data_table,role_code,logger)
            agncy_cd = str(int(new_image['agncy_cd']['S']))
            co_rep_code_list = get_com_rep_code(co_rep_data_table,emp_type,emp_num,logger)
            logger.info(f"Rep code list : {co_rep_code_list}")
            
            if employee_already_for_emptyp_marketid(co_rep_data_table,agncy_cd,emp_type,market_id,emp_num,logger):
                logger.info("Nothing to modify")
                return 0
            
            if len(co_rep_code_list) == 0:
                if emp_type in ['44','49','51','30']:
                    logger.info(f"emp_type : {emp_type} is 44 , 49 ,51 or 30")
                    co_rep_cd = valid_emp['op_id']
                
                else:
                    lastname = rep_detail['M']['co_rep_nm']['S'].split(" ")[-1]
                    if len(lastname)>2:
                        lastname_3_char = lastname[0:3].upper()
                        rep_cd_list = get_rep_cd_list(co_rep_data_table,lastname_3_char,logger)
                        
                        if len(rep_cd_list) == 0 :
                            co_rep_cd = lastname_3_char+'0'
                        else:
                            rep_cd_list = [d for d in rep_cd_list if d['co_rep_cd'][3:].isdigit()]
                            if len(rep_cd_list) == 0:
                                    max_id = -1
                            else:
                                rep_cd_list.sort(key = lambda rep_cd: rep_cd['co_rep_cd'],reverse = True)
                                max_id = int(rep_cd_list[0]['co_rep_cd'][3:])
                            co_rep_cd = lastname_3_char + str(max_id+1)
                            
                        logger.info(f"Rep code is generated with first 3 char of lastname + Next Seq available")
                    else:
                        lastname = lastname.upper()
                        rep_cd_list = get_rep_cd_list(co_rep_data_table,lastname,logger)
                        if len(rep_cd_list) == 0 :
                            co_rep_cd = lastname+'0'
                        else:
                            if(len(lastname) == 2):
                                rep_cd_list = [d for d in rep_cd_list if d['co_rep_cd'][2:].isdigit()]
                                if len(rep_cd_list) == 0:
                                    max_id = -1
                                else:
                                    rep_cd_list.sort(key = lambda rep_cd: rep_cd['co_rep_cd'],reverse = True)
                                    max_id = int(rep_cd_list[0]['co_rep_cd'][2:])
                            elif(len(lastname) == 1):
                                rep_cd_list = [d for d in rep_cd_list if d['co_rep_cd'][1:].isdigit()]
                                if len(rep_cd_list) == 0:
                                    max_id = -1
                                else:
                                    rep_cd_list.sort(key = lambda rep_cd: rep_cd['co_rep_cd'],reverse = True)
                                    max_id = int(rep_cd_list[0]['co_rep_cd'][1:])
                            co_rep_cd = lastname + str(max_id+1)
                            
                        logger.info(f"Rep code is generated with first 3 char of lastname + Next Seq available")
            else:
                logger.info(f"Rep code list is not empty for emp_num: {emp_num} and emp_type:{emp_type} i.e.{co_rep_code_list[0]}")
                co_rep_cd = co_rep_code_list[0]['co_rep_cd']
            
            logger.info(f"co_rep_cd : {co_rep_cd}")
            
            if is_rows_present(co_rep_data_table,agncy_cd,emp_type,'0',logger):
                delete_market_0(co_rep_data_table,agncy_cd,emp_type,logger)
                
            if is_rows_present(co_rep_data_table,agncy_cd,emp_type,market_id,logger):
                logger.info(f"Row already present.. Modifying for addition")
                modify_agncy_emp_typ(co_rep_data_table,agncy_cd,emp_type,market_id,'N',co_rep_cd,emp_num,logger)
                
                
            else:        
                co_rep_json = {
                  "agncy_cd": agncy_cd.rjust(7, '0'),
                  "agncy_nbr_cd": agncy_cd,
                  "co_rep_cd": co_rep_cd,
                  "creatd_by_id": "app_admin",
                  "create_dttm": get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S'),
                  "del_filtr_nbr": "nan",
                  "deleted_flag": "N",
                  "eff_dt": get_today_loc_dt().strftime('%Y-%m-%d'),
                  "emp_id": emp_num,
                  "employee_type_cd": emp_type,
                  "gs1pk": "empno#{}".format(emp_num),
                  "gs1sk": "emptyp#{}#agncy#{}#markt#{}".format(emp_type,agncy_cd,market_id),
                  "gs2pk": "rep#{}".format(co_rep_cd),
                  "gs2sk": "agcy#{}".format(agncy_cd),
                  "markt_cd": market_id,
                  "markt_desc": co_rep_desc,
                  "pk": "agcy#{}".format(agncy_cd),
                  "sk": "emptyp#{}#markt#{}".format(emp_type,market_id),
                  "updt_by_id": "app_admin",
                  "updt_dttm": get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                response = co_rep_data_table.put_item(
                Item= co_rep_json
                )
            
            logger.info(f"updated for agncy_cd : {agncy_cd}, emp_num: {emp_num} and emp_type : {emp_type}")
        
        else:
            logger.error(f"{emp_num} :Employee doesnot exiists in the employee table")
            
    except Exception as ex:
        log_message = f"Exception occurred in add_co_rep_data {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
            
        
def delete_co_rep_data(rep_detail,old_image,cross_accnt_session,logger):
    try:
        logger.info(f"Beigin to delete employee in co_rep_dat DDB")
        emp_num = str(int(rep_detail['M']['co_rep_cd']['S']))
        valid_emp = valid_employee(emp_num,cross_accnt_session,logger)
        if valid_emp != False:
            logger.info(f"{emp_num} :Valid employee")
            market_id = rep_detail['M']['lob_id']['S']
            co_rep_desc = rep_detail['M']['lob_nm']['S']
            role_code = rep_detail['M']['org_role_cd']['S']
            emp_type = get_emp_type(co_rep_data_table,role_code,logger)
            agncy_cd = old_image['agncy_cd']['S']
            
            if is_rows_present(co_rep_data_table,agncy_cd,emp_type,'0',logger):
                delete_market_0(co_rep_data_table,agncy_cd,emp_type,logger)
            
            if is_rows_present(co_rep_data_table,agncy_cd,emp_type,market_id,logger):
                logger.info(f"Row already present.. Modifying for deletion")
                modify_agncy_emp_typ(co_rep_data_table,agncy_cd,emp_type,market_id,'Y','UNAS','0',logger)
                
            else:
                co_rep_json = {
                  "agncy_cd": agncy_cd.rjust(7, '0'),
                  "agncy_nbr_cd": agncy_cd,
                  "co_rep_cd": "UNAS",
                  "creatd_by_id": "app_admin",
                  "create_dttm": get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S'),
                  "del_filtr_nbr": "nan",
                  "deleted_flag": "Y",
                  "eff_dt": get_today_loc_dt().strftime('%Y-%m-%d'),
                  "emp_id": "0",
                  "employee_type_cd": emp_type,
                  "gs1pk": "empno#{}".format("0"),
                  "gs1sk": "emptyp#{}#agncy#{}#markt#{}".format(emp_type,agncy_cd,market_id),
                  "gs2pk": "rep#{}".format("UNAS"),
                  "gs2sk": "agcy#{}".format(agncy_cd),
                  "markt_cd": market_id,
                  "markt_desc": co_rep_desc,
                  "pk": "agcy#{}".format(agncy_cd),
                  "sk": "emptyp#{}#markt#{}".format(emp_type,market_id),
                  "updt_by_id": "app_admin",
                  "updt_dttm": get_today_loc_dt().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                response = co_rep_data_table.put_item(
                Item= co_rep_json
                )
            logger.info(f"Record has been modified as part of deletion i.e agncy_cd : {agncy_cd}, emp_num: {emp_num} and emp_type : {emp_type}")
            
        else:
            logger.error(f"{emp_num} :Employee doesnot exiists in the employee table")
    
    except Exception as ex:
        log_message = f"Exception occurred in delete_co_rep_data {ex} \n {traceback.format_exc()}"
        logger.error(log_message)
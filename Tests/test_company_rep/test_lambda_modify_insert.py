from datetime import  datetime
from boto3.dynamodb.conditions import Key
import logging
from mock import patch
import mock
import pandas as pd
import os


os.environ['TABLE_NAME']='test'
from ent_sls_adj_common_util.functions.company_rep import checks
from ent_sls_adj_common_util.functions.company_rep import cross_account_session


# Creaate a logger and setting the logging level
logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)
def ret_d(dynamodb):
    return dynamodb




def get_emp_history_data(table, agncy_cd,logger):
    try:
        response = table.query(
            KeyConditionExpression=Key('pk').eq("agcy#{}".format(agncy_cd))
        )
        items = response['Items']

        return items
    except Exception as ex:
        log_message = f"Exception occurred in get_emp_history_data{ex} "
        logger.error(log_message)

@patch('cross_account_session.get_cross_account_session',mock.MagicMock(return_value=None))
@patch('checks.execute_query_from_athena',mock.MagicMock(return_value = pd.read_csv('Tests/test_company_rep/athena_data/data.csv', dtype=str)))
@patch('checks.get_today_loc_dt',mock.MagicMock(return_value=datetime(2021, 5, 18)))
@patch('checks.employee_already_for_emptyp_marketid',mock.MagicMock(return_value=False))
def test_lambda_handler_modify(table):
    from .input_map import input_dict
    from .result_map import test_lambda_handler_modify_result
    from ent_sls_adj_common_util.functions.company_rep.lambda_function import lambda_handler
    checks.co_rep_data_table = table
    ret = lambda_handler(input_dict,None)
    items = get_emp_history_data(checks.co_rep_data_table,'6502',logger)
    logger.info(items)
    assert items == test_lambda_handler_modify_result

@patch('cross_account_session.get_cross_account_session',mock.MagicMock(return_value=None))
@patch('checks.execute_query_from_athena',mock.MagicMock(return_value = pd.read_csv('Tests/test_company_rep/athena_data/data.csv', dtype=str)))
@patch('checks.get_today_loc_dt',mock.MagicMock(return_value=datetime(2021, 5, 18)))
@patch('checks.employee_already_for_emptyp_marketid',mock.MagicMock(return_value=False))
def test_lambda_handler_insert(table):
    from .input_map import input_dict_insert
    from ent_sls_adj_common_util.functions.company_rep.lambda_function import lambda_handler
    from .result_map import test_lambda_handler_insert_result
    checks.co_rep_data_table = table
    ret = lambda_handler(input_dict_insert,None)
    items = get_emp_history_data(table,'6502',logger)
    logger.info(items)
    assert items == test_lambda_handler_insert_result

@patch('cross_account_session.get_cross_account_session',mock.MagicMock(return_value=None))
@patch('checks.execute_query_from_athena',mock.MagicMock(return_value = pd.read_csv('Tests/test_company_rep/athena_data/data.csv', dtype=str)))
@patch('checks.get_today_loc_dt',mock.MagicMock(return_value=datetime(2021, 5, 18)))
@patch('checks.employee_already_for_emptyp_marketid',mock.MagicMock(return_value=False))
def test_lambda_handler_emp_lastname_less_3_char(table):
    from .input_map import input_dict_insert_emp_lastname_less_3_char
    from ent_sls_adj_common_util.functions.company_rep.lambda_function import lambda_handler
    from .result_map import test_lambda_handler_emp_lastname_less_3_char_result
    checks.co_rep_data_table = table
    ret = lambda_handler(input_dict_insert_emp_lastname_less_3_char,None)
    items = get_emp_history_data(table,'6502',logger)
    logger.info(items)

    assert items == test_lambda_handler_emp_lastname_less_3_char_result

@patch('cross_account_session.get_cross_account_session',mock.MagicMock(return_value=None))
@patch('checks.execute_query_from_athena',mock.MagicMock(return_value = pd.read_csv('Tests/test_company_rep/athena_data/empty_data.csv', dtype=str)))
@patch('checks.get_today_loc_dt',mock.MagicMock(return_value=datetime(2021, 5, 18)))
@patch('checks.employee_already_for_emptyp_marketid',mock.MagicMock(return_value=False))
def test_lambda_handler_emp_not_present(table):
    from .input_map import input_dict_insert_emp_lastname_less_3_char
    from ent_sls_adj_common_util.functions.company_rep.lambda_function import lambda_handler
    from .result_map import test_lambda_handler_emp_lastname_less_3_char_result
    checks.co_rep_data_table = table
    ret = lambda_handler(input_dict_insert_emp_lastname_less_3_char,None)
    items = get_emp_history_data(table,'6502',logger)
    logger.info(items)
    assert items == []
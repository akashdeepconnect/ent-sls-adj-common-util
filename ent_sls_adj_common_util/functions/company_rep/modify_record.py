from checks import add_co_rep_data
from checks import delete_co_rep_data


def modify_handler(record,cross_accnt_session,logger):
    new_image = record['dynamodb']['NewImage']
    old_image = record['dynamodb']['OldImage']
    co_rep_map_new = new_image['co_rep_map']['L']
    co_rep_map_old = old_image['co_rep_map']['L']
    
    
    for rep_detail in co_rep_map_old:
        if rep_detail not in co_rep_map_new:
            delete_co_rep_data(rep_detail,old_image,cross_accnt_session,logger)
    
    for rep_detail in co_rep_map_new:
        if rep_detail not in co_rep_map_old:
            add_co_rep_data(rep_detail,new_image,cross_accnt_session,logger)
   
from checks import add_co_rep_data


def insert_handler(record,cross_accnt_session,logger):
    new_image = record['dynamodb']['NewImage']
    co_rep_map_new = new_image['co_rep_map']['L']
    
    
    for rep_detail in co_rep_map_new:
        add_co_rep_data(rep_detail,new_image,cross_accnt_session,logger)
            

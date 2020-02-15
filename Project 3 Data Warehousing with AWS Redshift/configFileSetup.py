import configparser

config = configparser.ConfigParser()

config['aws_credentials'] = {
     'access_key_id': ''
    ,'secret_access_key': ''
}

config['cluster_settings'] = {
     'redshift_role_name':'dwhRole'
    ,'redshift_role_arn':''
    ,'cluster_type':'multi-node'
    ,'number_of_nodes':'4'
    ,'node_type':'dc2.large'
    ,'cluster_identifier':'dwhCluster'
    ,'db_name':'sparkify_dw'
    ,'master_user_name':'sparkify'
    ,'master_user_password':
    ,'port':'5439'
    ,'endpoint':''
    ,
}

config['s3'] = {
     'log_data':'s3://udacity-dend/log_data'
    ,'log_jsonpath':'s3://udacity-dend/log_json_path.json'
    ,'song_data':'s3://udacity-dend/song_data'
}

# write credentials into a ".cfg" file
with open('dwh.cfg','w') as credentials_file:
    config.write(credentials_file)
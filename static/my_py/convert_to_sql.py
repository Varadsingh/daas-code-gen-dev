import pandas as pd
import os, datetime,glob,ast
from pathlib import Path

def convert(**kwargs):
    
    st_time = str(datetime.datetime.now())
    st_time = st_time.split('.')[0].replace(' ','').replace(':','').replace('-','')

    run = kwargs['run']
    file = kwargs['file']
    foldname = kwargs['foldname']
    bridge = kwargs['bridge']
    kafka = kwargs['kafka']
    materialize = kwargs['materialize']
    sandbox = kwargs['sandbox']

    if 'kafka_value' in kwargs:
        kafka_value = kwargs['kafka_value']
    else:
        kafka_value = None
    
    if 'materialize_value' in kwargs:
        materialize_value = kwargs['materialize_value']
    else:
        materialize_value = None
    if 'kafka_value' in kwargs:
        kafka_value = kwargs['kafka_value']
    else:
        kafka_value = None
    
    if 'bridge_value' in kwargs:
        bridge_value = kwargs['bridge_value']
    else:
        bridge_value = None
    

    try:
        matip = kwargs['matip']
        matport = kwargs['matport']
        matuser = kwargs['matuser']
        matpass = kwargs['matpass']
        matdb = kwargs['matdb']
    except Exception as e:
        matip = "3.14.255.156"
        matport = "6875"
        matuser = "materialize"
        matpass = "materialize"
        matdb = "materialize"
        

    file_name = os.path.basename(file).upper()
    
    source_name = file_name.replace(".CSV","").replace("_DATA_TYPES","").replace("API_","")
    view_name = file_name.replace(".CSV","").replace("_DATA_TYPES","")
    fname = file_name.split('.')[0].replace('_DATA_TYPES','')
    csv_file_number = file_name[0:2]
    # print(type(csv_file_number), type(source_name[:source_name.index('_')]))
    if type(int(source_name[:source_name.index('_')])) == type(1):
        source_name = source_name[source_name.index('_')+1:]
    if type(int(view_name[:view_name.index('_')])) == type(1):
        view_name = view_name[view_name.index('_')+1:]

    source_df = pd.read_csv(file,dtype=str)

    cast_prefix = "cast(payload::jsonb->>'"

    ts_prefix = "to_timestamp("
    ts_postfix = "/1000)"

    tz_prefix = "("
    tz_postfix = "::timestamp with time zone)"

    char_prefix = "to_char("
    char_postfix = ", 'YYYY-MM-DD HH24:MI:SS.MS+TZ')"

    len_check_prefix = "CASE LENGTH(payload::jsonb->>'"
    len_check_mid = "') WHEN (0) THEN NULL ELSE "
    len_check_postfix = " END"


    databridge_host =  "'host="+matip+" port="+matport+" dbname="+matdb+" user="+matuser+" password="+matpass+"'"
    databridge_pre = "\nSELECT dblink_connect_u('a',"+databridge_host+");"
    
    databridge_prefix_1 = "\n\nDROP TABLE IF EXISTS " + view_name + " CASCADE;\n\nCREATE TABLE " + view_name + " AS SELECT * FROM dblink('a','SELECT "
    databridge_mid_1 = " FROM public." + view_name + "') as T("
    databridge_postfix_1 = ");"

    source_df.iloc[:,1] = list(map(lambda x: x.lower(), source_df.iloc[:,1]))

    select_body = ""
    databridge_select_cols = ""
    databridge_t_cols = ""


    for i in range(0,len(source_df)):
        col_name = source_df.iloc[i,0].strip()
        
        select_body = select_body + "\n"
        
        try:
            data_size = source_df.iloc[i,1].split('(')[1].split(')')[0].strip() # CHG_20210902
            data_size = '(' + data_size + ')'
        except:
            data_size = ''
        
        if 'varchar' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'string'
            source_df.iloc[i,4] = 'String'
            source_df.iloc[i,5] = 'varchar' + data_size # CHG_20210902
            source_df.iloc[i,6] = 'String'
            
            select_body = select_body + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")"
            
            databridge_t_cols = databridge_t_cols + col_name + " varchar" + data_size # CHG_20210902
            databridge_select_cols = databridge_select_cols + col_name + ','
            
        elif 'char' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'string'
            source_df.iloc[i,4] = 'String'
            source_df.iloc[i,5] = 'char' + data_size # CHG_20210902
            source_df.iloc[i,6] = 'String'
            
            select_body = select_body + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")"
            
            databridge_t_cols = databridge_t_cols + col_name + " char" + data_size # CHG_20210902
            databridge_select_cols = databridge_select_cols + col_name + ','
            
        elif 'time' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = '"type": "int64", "name": "org.apache.kafka.connect.data.Timestamp"'
            source_df.iloc[i,4] = 'float'
            source_df.iloc[i,5] = 'timestamp' # CHG_20210902
            source_df.iloc[i,6] = 'String'
            
            select_body = select_body + char_prefix + tz_prefix + ts_prefix + cast_prefix + col_name + "' as "+ source_df.iloc[i,4] + ")" + ts_postfix + tz_postfix + char_postfix
            
            databridge_t_cols = databridge_t_cols + col_name + " varchar(30)" # CHG_20210902 timestamp
            databridge_select_cols = databridge_select_cols + 'TO_TIMESTAMP(' + col_name + ",'YYYY-MM-DD HH24:MI:SS.MS') AS " + col_name + ','

        elif 'date' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'string'
            source_df.iloc[i,4] = 'Date'
            source_df.iloc[i,5] = 'date' # CHG_20210902
            source_df.iloc[i,6] = 'String'
            
            select_body = select_body + len_check_prefix + col_name + len_check_mid + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")" + len_check_postfix
            
            databridge_t_cols = databridge_t_cols + col_name + " date"
            databridge_select_cols = databridge_select_cols + col_name + ','

        elif 'int' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'int32'
            source_df.iloc[i,4] = 'float'
            source_df.iloc[i,5] = 'int8' # CHG_20210902
            source_df.iloc[i,6] = 'Int'
            
            select_body = select_body + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")"

            databridge_t_cols = databridge_t_cols + col_name + " int8"# CHG_20210902            
            databridge_select_cols = databridge_select_cols + col_name + ','

        elif 'decimal' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'double'
            source_df.iloc[i,4] = 'float'
            source_df.iloc[i,5] = 'decimal' + data_size # CHG_20210902
            source_df.iloc[i,6] = 'Float'
            
            select_body = select_body + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")"

            databridge_t_cols = databridge_t_cols + col_name + " decimal" + data_size # CHG_20210902            
            databridge_select_cols = databridge_select_cols + col_name + ','

        elif 'float' in source_df.iloc[i,1]:
            source_df.iloc[i,3] = 'double'
            source_df.iloc[i,4] = 'float'
            source_df.iloc[i,5] = 'decimal' + data_size # CHG_20210902
            source_df.iloc[i,6] = 'Float'
            
            select_body = select_body + cast_prefix + col_name
            select_body = select_body + "' as "+ source_df.iloc[i,4] + ")"

            if data_size == '':
                databridge_t_cols = databridge_t_cols + col_name + " float" # CHG_20210902
            else:
                databridge_t_cols = databridge_t_cols + col_name + " decimal" + data_size # CHG_20210902            
            databridge_select_cols = databridge_select_cols + col_name + ','

        else:
            select_body = select_body + cast_prefix + col_name
            select_body = "--"+ select_body + "' as UNKOWN DATATYPE)"
            databridge_t_cols = databridge_t_cols + col_name + " unkown"
            databridge_select_cols = databridge_select_cols + col_name + ','
            
        select_body = select_body + " as " + col_name + ","
        databridge_t_cols = databridge_t_cols + ','

        
    select_body = select_body[:-1]
    select_body = select_body + "\n" 
    databridge_select_cols = databridge_select_cols[:-1]
    databridge_t_cols = databridge_t_cols[:-1]

    sql_query = select_body
    
    main_dir = os.path.dirname(os.path.dirname(os.path.dirname(file)))
    if not os.path.exists(main_dir + '\\generated\\csv'):
        os.makedirs(main_dir + '\\generated\\csv')
    
    file_excel = main_dir + '\\generated\\csv\\'  + file_name
    
    print(file_excel)

    source_df.to_csv(file_excel, index=False)

    kafka_val = "'b-1.daas-api-poc-kafka-clu.2688ax.c4.kafka.us-east-1.amazonaws.com:9092,b-2.daas-api-poc-kafka-clu.2688ax.c4.kafka.us-east-1.amazonaws.com:9092'"
    
    databridge_query = databridge_pre

    databridge_query = databridge_query + databridge_prefix_1 + databridge_select_cols + databridge_mid_1 + databridge_t_cols + databridge_postfix_1 + '\n\n'

    
    if foldname != None and run == 0:
        parent_folder = str(Path(foldname).parent)
        param_file = parent_folder + '/' + 'code_gen_param.xlsx'
        
        fk_pk_df = pd.read_excel(param_file, keep_default_na=False, sheet_name = 'code_gen_param')
        
        fk_pk_rows = fk_pk_df[fk_pk_df['num'] == int(csv_file_number)]
        fk_query = ''
        pk_query = ''
        fk_pk_rows.reset_index(inplace=True,drop=True)
        if len(fk_pk_rows) > 0:
            for i in range(0,len(fk_pk_rows)):
                table = fk_pk_rows['source_table_name'][i]
                tera_view_name = fk_pk_rows['tera_view_name'][i]
                kafka_col_name = fk_pk_rows['kafka_timestamp_column_name'][i]
                fields = fk_pk_rows['kakfa_transforms_createkey_fields'][i]
                kafka_query_rows = fk_pk_rows['kafka_query_rows'][i]
                file_num = fk_pk_rows['num'][i]
                bool_value = fk_pk_rows['boolean'][i]
                
                df_fkpk = pd.read_excel(param_file, keep_default_na=False, sheet_name = 'code_gen_param_pk_fk')
                rows_fkpk = df_fkpk[df_fkpk['source_table_name'] == view_name.lower()]
                keys_count = len(list(set(rows_fkpk['constraint_name'])))
                keys = list(set(rows_fkpk['constraint_name']))
                fk_query = ''
                pk_query = ''
                rows_fkpk.reset_index(inplace=True,drop=True)
                
                for key in keys:
                    fk_pk_rows_key = rows_fkpk[rows_fkpk['constraint_name'] == key]
                    fk_pk_rows_key.reset_index(inplace=True,drop=True)
                    
                    if len(fk_pk_rows_key) > 0:
                        table = fk_pk_rows_key['source_table_name'][0]
                        ref_table_name = fk_pk_rows_key['reference_table_name'][0]
                        constraint_type = fk_pk_rows_key['constraint_type'][0]
                        constraint_name = fk_pk_rows_key['constraint_name'][0]

                        col_names = ''
                        ref_col_names = ''
                        for i in range(0,len(fk_pk_rows_key)):
                            col_names = col_names + fk_pk_rows_key['source_column_name'][i] + ","
                            ref_col_names = ref_col_names + fk_pk_rows_key['reference_column_name'][i] + ","
                            
                        col_names = col_names[:-1]
                        ref_col_names = ref_col_names[:-1]
                        
                        if constraint_type == "primary key":
                            pk_query = pk_query + 'ALTER TABLE ' + table + ' ADD PRIMARY KEY (' + col_names + ');\n'
                        if constraint_type == "foreign key":
                            fk_query = fk_query + 'ALTER TABLE ' + table + ' ADD CONSTRAINT ' + constraint_name + ' FOREIGN KEY (' + col_names + ') REFERENCES ' + ref_table_name + '(' + ref_col_names + ') NOT VALID;\n'

            if pk_query != '' and fk_query == '':            
                alter_query = pk_query
            elif pk_query != '' and fk_query != '':            
                alter_query = pk_query + '\n' + fk_query
            elif pk_query == '' and fk_query != '':            
                alter_query = fk_query
            else:
                alter_query = ''
                

            if bool_value == True:
                
                kafka_value = kafka_value.replace("__TERA_VIEW_NAME_LOW__",tera_view_name.lower()).replace("__TERA_VIEW_NAME__",tera_view_name.upper()).replace("__KAFKA_TIMESTAMP__", kafka_col_name).replace("__TERA_PRIMARY_KEYS__", fields).replace("__KAFKA_QUERY_ROWS__",str(kafka_query_rows))

                materialize_value = materialize_value.replace("__TERA_VIEW_NAME__",tera_view_name.upper()).replace("__M-VIEW-NAME__",table.upper()).replace('-- INSERT CODE HERE --',select_body)

                databridge_host =  "'host="+matip+" port="+matport+" dbname="+matdb+" user="+matuser+" password="+matpass+"'"
                bridge_value = bridge_value.replace('__SED_BRIDGE_DB_HOST__', databridge_host).replace('__M-VIEW-NAME__', view_name).replace('__DATABRIDGE_SELECT_COLS__', databridge_select_cols).replace('__DATABRIDGE_T_COLS__', databridge_t_cols).replace('__ALTER_QUERY__', alter_query.rstrip())

                if file_num in range(1,10):
                    file_number = "0" + str(file_num)
                else:
                    file_number = str(file_num)
                    
                json_file_name = file_number + '_' + tera_view_name.upper()
                
                f = open(kafka + '/' + json_file_name + '-JDBC-Connector.json' , 'w')
                f.write(kafka_value)
                f.close()

                f1 = open(materialize + '/' + file_number+ '_' + source_name.lower() + '_materialize_sed.sql', 'w')
                f1.write(materialize_value)
                f1.close()
                
                f2 = open(sandbox + '/'  + file_number+ '_' + source_name.lower() + '_materialize.sql', 'w')
                f2.write(materialize_value.replace('__SED_BROKER_URL__',kafka_val))
                f2.close()
                
                f = open(sandbox + '/' + file_number + '_' + source_name.lower() + '_bridge.sql', 'w')
                f.write("SELECT dblink_disconnect('a');\n"+bridge_value)
                f.close()

                f = open(bridge + '/' + file_number + '_' + source_name.lower() + '_bridge_sed.sql', 'w')
                f.write(bridge_value.replace(databridge_host,'__SED_BRIDGE_DB_HOST__'))
                f.close()
            else:
                print("files is not created for file number " +str(file_num)+ " because it has false value of boolean")
    else:
                        
        user_value = materialize_value.replace('-- INSERT CODE HERE --',select_body)
        f = open(materialize + '/' + fname.lower() + '_materialize_sed.sql', 'w')
        f.write(user_value)
        f.close()
        
        f = open(sandbox + '/'  + fname.lower() + '_materialize.sql', 'w')
        f.write(user_value.replace('__SED_BROKER_URL__',kafka_val))
        f.close()
    
        bridge_value = bridge_value.replace('__M-VIEW-NAME__', view_name).replace('__DATABRIDGE_SELECT_COLS__', databridge_select_cols).replace('__DATABRIDGE_T_COLS__', databridge_t_cols).replace('__ALTER_QUERY__\n', '')
        f = open(sandbox + '/' + fname.lower() + '_bridge.sql', 'w')
        f.write("SELECT dblink_disconnect('a');\n"+bridge_value)
        f.close()

        f = open(bridge + '/' + fname.lower() + '_bridge_sed.sql', 'w')
        f.write(bridge_value.replace(databridge_host,'__SED_BRIDGE_DB_HOST__'))
        f.close()
    
    return([sql_query, bridge_value])





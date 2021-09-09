import requests
import pandas as pd
import json
HASURA_URL = "http://18.220.36.162:8080/v2/query"


def convert_yaml(yaml_file_path):
    fetchFKrelation = "SELECT tc.table_schema, tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_schema AS foreign_table_schema, ccu.table_name AS foreign_table_name,ccu.column_name AS foreign_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu  ON tc.constraint_name = kcu.constraint_name  AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';"
    fetchTable = "SELECT table_name FROM information_schema.tables where table_schema='public'"
    
    tb_body = {
            "type": "run_sql",
            "args": {
                "source": "default",
                "sql": fetchTable
            }
        }
    
    pk_fk_body =  {
            "type": "run_sql",
            "args": {
                "source": "default",
                "sql": fetchFKrelation
            }}
    
    res = requests.post(HASURA_URL,data=json.dumps(tb_body), headers={ "Content-Type": "application/json" }).json()
    table_list = []
    for re in res['result'][1:]:
        for r in re:
            table_list.append(r)
    sorted_table_list = sorted(table_list)
    tb_df = pd.DataFrame(sorted_table_list)
    tb_df.columns = ['table_name']

    pk_fk_list = requests.post(HASURA_URL,data=json.dumps(pk_fk_body), headers={ "Content-Type": "application/json" }).json()
    pk_fk_df = pd.DataFrame(pk_fk_list['result']) 
    pk_fk_header = pk_fk_df.iloc[0]
    pk_fk_df = pk_fk_df[1:]
    pk_fk_df.columns = pk_fk_header

    file_str = ''
    for i, p in tb_df.iterrows():
        
        table_name = p['table_name']
        table_str = f"- table:\n    schema: public\n    name: {table_name}\n"
        
        
        pk_fk_row_array = pk_fk_df[pk_fk_df['foreign_table_name'] == table_name]
        pk_fk_row_array.reset_index(inplace=True,drop=True)
        

        array_relationship_str = f"  array_relationships:\n"
        object_relationship_str = f"  object_relationships:\n"
        
        if len(pk_fk_row_array) != 0:
            for j in range(0,len(pk_fk_row_array)):
                ref_col_name = pk_fk_row_array['column_name'][j]
                ref_table_name = pk_fk_row_array['table_name'][j]
                foreign_table_name = pk_fk_row_array['foreign_table_name'][j]
                
                array_relationship_str += f"  - name: {ref_table_name}s\n    using:\n      foreign_key_constraint_on:\n        column: {ref_col_name}\n        table:\n          schema: public\n          name: {ref_table_name}\n"
        else:
            array_relationship_str = ""
                
                
        pk_fk_row = pk_fk_df[pk_fk_df['table_name'] == p['table_name']]
        pk_fk_row.reset_index(inplace=True,drop=True)
        if len(pk_fk_row) != 0:
            for j in range(0,len(pk_fk_row)):
                col_name = pk_fk_row['column_name'][j]
                table_name = pk_fk_row['table_name'][j]
                foreign_table_name = pk_fk_row['foreign_table_name'][j]
                
                object_relationship_str += f"  - name: {table_name}s\n    using:\n      foreign_key_constraint_on: {col_name}\n"
        else:
            object_relationship_str = ""
        file_str += table_str + array_relationship_str +object_relationship_str

    
    f = open(yaml_file_path, "w")
    f.write(file_str)
    
    return ""
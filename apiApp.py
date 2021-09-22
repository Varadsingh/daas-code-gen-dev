import os, datetime, time, glob
import psycopg2, json
from flask import request, jsonify, Flask, render_template, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
from static.my_py.convert_to_sql import convert
from static.my_py.generate_yaml_file import convert_yaml
import dotenv
import subprocess

dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
import shutil
import os
foldName = os.getenv('source')
bridgePath = os.getenv('bridge')
kafkaPath = os.getenv('kafka')
materializePath = os.getenv('materialize')
sandboxPath = os.getenv('sandbox')
bridgeIP = os.getenv("bridgeip")
bridgePort = os.getenv("bridgeport")
bridgeUser = os.getenv("bridgeuser")
bridgePass = os.getenv("bridgepass")
bridgeDB = os.getenv("bridgedb")
matIP = os.getenv("matip")
matPort = os.getenv("matport")
matUser = os.getenv("matuser")
matPass = os.getenv("matpass")
matDB = os.getenv("matdb")
shellScriptFile = os.getenv("shellScriptFile")
hasuraYamlFile = os.getenv("hasuraYamlFile")
hasuraCLIPath = os.getenv("hasuraCLIPath")

print(foldName)

app = Flask(__name__)
CORS(app)

# base_dir = str(os.getcwd())
base_dir = 'C:/files'
    
UPLOAD_FOLDER = base_dir + '/Uploaded/'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def createConn(host,port,database,user,password):

    global conn, cursor
    
    conn = psycopg2.connect(
    host=host,
    port=port,
    database=database,
    user=user,
    password=password)

    conn.autocommit = True

    cursor = conn.cursor()
    
@app.route('/')
def home():
    return render_template('index.html')
    
@app.route('/single_run')
def single_run():
    context = {
        'matIP' : matIP,
        'matPort' : matPort,
        'matUser': matUser,
        'matPass': matPass,
        'matDB': matDB
    }
    return render_template('single_run.html', context=context)
    
@app.route('/batch_run')
def batch_run():

    context = {
        'source' : foldName,
        'bridge' : bridgePath,
        'kafka' : kafkaPath,
        'materialize' : materializePath,
        'sandbox' : sandboxPath,
        'matIP' : matIP,
        'matPort' : matPort,
        'matUser': matUser,
        'matPass': matPass,
        'matDB': matDB
    }
    
    return render_template('batch_run.html', context=context)

@app.route('/sql_ops')
def sql_ops():
    context = {
        'bridgeIP' : bridgeIP,
        'bridgePort' : bridgePort,
        'bridgeUser': bridgeUser,
        'bridgePass': bridgePass,
        'bridgeDB': bridgeDB
    }
    return render_template('sql_ops.html', context=context)

@app.route('/getTables', methods=['POST'])
def getTables():
    global conn, cursor
    bridgeip = request.form['bridgeip']
    if bridgeip == bridgeIP:
        bridgeip = bridgeip
    else:
        bridgeip = bridgeip
        os.environ['bridgeip'] = bridgeip
        dotenv.set_key(dotenv_file, "bridgeip", os.environ["bridgeip"], quote_mode='always')
        
    bridgeport = request.form['bridgeport']
    if bridgeport == bridgePort:
        bridgeport = bridgeport
    else:
        bridgeport = bridgeport
        os.environ['bridgeport'] = bridgeport
        dotenv.set_key(dotenv_file, "bridgeport", os.environ["bridgeport"], quote_mode='always')

    bridgeuser = request.form['bridgeuser']
    if bridgeuser == bridgeUser:
        bridgeuser = bridgeuser
    else:
        bridgeuser = bridgeuser
        os.environ['bridgeuser'] = bridgeuser
        dotenv.set_key(dotenv_file, "bridgeuser", os.environ["bridgeuser"], quote_mode='always')
        
    bridgepass = request.form['bridgepass']
    if bridgepass == bridgePass:
        bridgepass = bridgepass
    else:
        bridgepass = bridgepass
        os.environ['bridgepass'] = bridgepass
        dotenv.set_key(dotenv_file, "bridgepass", os.environ["bridgepass"], quote_mode='always')
            
    bridgedb = request.form['bridgedb']
    if bridgedb == bridgeDB:
        bridgedb = bridgedb
    else:
        bridgedb = bridgedb
        os.environ['bridgedb'] = bridgedb
        dotenv.set_key(dotenv_file, "bridgedb", os.environ["bridgedb"], quote_mode='always')
    createConn(bridgeip,bridgeport,bridgedb,bridgeuser,bridgepass)
    
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name asc;")
    rows = cursor.fetchall()
    rows_json = json.dumps(rows)
    print(rows_json)
    conn.close()
    return({'rows':rows})

@app.route('/getCols', methods=['POST'])
def getCols():
    global conn, cursor
    
    bridgeip = request.form['bridgeip']
    if bridgeip == bridgeIP:
        bridgeip = bridgeip
    else:
        bridgeip = bridgeip
        os.environ['bridgeip'] = bridgeip
        dotenv.set_key(dotenv_file, "bridgeip", os.environ["bridgeip"], quote_mode='always')
        
    bridgeport = request.form['bridgeport']
    if bridgeport == bridgePort:
        bridgeport = bridgeport
    else:
        bridgeport = bridgeport
        os.environ['bridgeport'] = bridgeport
        dotenv.set_key(dotenv_file, "bridgeport", os.environ["bridgeport"], quote_mode='always')

    bridgeuser = request.form['bridgeuser']
    if bridgeuser == bridgeUser:
        bridgeuser = bridgeuser
    else:
        bridgeuser = bridgeuser
        os.environ['bridgeuser'] = bridgeuser
        dotenv.set_key(dotenv_file, "bridgeuser", os.environ["bridgeuser"], quote_mode='always')
        
    bridgepass = request.form['bridgepass']
    if bridgepass == bridgePass:
        bridgepass = bridgepass
    else:
        bridgepass = bridgepass
        os.environ['bridgepass'] = bridgepass
        dotenv.set_key(dotenv_file, "bridgepass", os.environ["bridgepass"], quote_mode='always')
            
    bridgedb = request.form['bridgedb']
    if bridgedb == bridgeDB:
        bridgedb = bridgedb
    else:
        bridgedb = bridgedb
        os.environ['bridgedb'] = bridgedb
        dotenv.set_key(dotenv_file, "bridgedb", os.environ["bridgedb"], quote_mode='always')
        
    createConn(bridgeip,bridgeport,bridgedb,bridgeuser,bridgepass)
    
    table = request.form['table']
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + table + "' AND table_schema = 'public' ORDER BY table_name, ordinal_position;")
    rows = cursor.fetchall()
    rows_json = json.dumps(rows)
    print(rows)
    conn.close()
    return({'status':'Processing Completed','rows':rows})
    
@app.route('/runOp', methods=['POST'])
def runOp():
    global conn, cursor
    
    bridgeip = request.form['bridgeip']
    if bridgeip == bridgeIP:
        bridgeip = bridgeip
    else:
        bridgeip = bridgeip
        os.environ['bridgeip'] = bridgeip
        dotenv.set_key(dotenv_file, "bridgeip", os.environ["bridgeip"], quote_mode='always')
        
    bridgeport = request.form['bridgeport']
    if bridgeport == bridgePort:
        bridgeport = bridgeport
    else:
        bridgeport = bridgeport
        os.environ['bridgeport'] = bridgeport
        dotenv.set_key(dotenv_file, "bridgeport", os.environ["bridgeport"], quote_mode='always')

    bridgeuser = request.form['bridgeuser']
    if bridgeuser == bridgeUser:
        bridgeuser = bridgeuser
    else:
        bridgeuser = bridgeuser
        os.environ['bridgeuser'] = bridgeuser
        dotenv.set_key(dotenv_file, "bridgeuser", os.environ["bridgeuser"], quote_mode='always')
        
    bridgepass = request.form['bridgepass']
    if bridgepass == bridgePass:
        bridgepass = bridgepass
    else:
        bridgepass = bridgepass
        os.environ['bridgepass'] = bridgepass
        dotenv.set_key(dotenv_file, "bridgepass", os.environ["bridgepass"], quote_mode='always')
            
    bridgedb = request.form['bridgedb']
    if bridgedb == bridgeDB:
        bridgedb = bridgedb
    else:
        bridgedb = bridgedb
        os.environ['bridgedb'] = bridgedb
        dotenv.set_key(dotenv_file, "bridgedb", os.environ["bridgedb"], quote_mode='always')
        
    createConn(bridgeip,bridgeport,bridgedb,bridgeuser,bridgepass)
    
    operation = request.form['operation']
    table = request.form['table']
    col_names = request.form['col_names']
    op_name = request.form['op_name']
    if operation == 'create_foreign_key':
        ref_table_name = request.form['ref_table_name']
        ref_col_names = request.form['ref_col_names']
    else:
        ref_table_name = None
        ref_col_names = None
    
    try:
        if operation == 'create_primary_key':
            sql_query = 'ALTER TABLE ' + table + ' ADD PRIMARY KEY (' + col_names + ');'
            print(sql_query)
            cursor.execute(sql_query)
            status = 'Primary Key Created'
        if operation == 'create_foreign_key':
            sql_query = 'ALTER TABLE ' + table + ' ADD CONSTRAINT ' + op_name + ' FOREIGN KEY (' + col_names + ') REFERENCES ' + ref_table_name + '(' + ref_col_names + ') NOT VALID;'
            print(sql_query)
            cursor.execute(sql_query)
            status = 'Foreign Key ' + op_name + ' Created'
        
    except Exception as e:
        status = str(e)
    
    conn.close()
    return({'status':status})
    
@app.route('/runStatus', methods=['POST'])
def runStatus():
    time.sleep(1)
    print('checking status')
    foldname = request.form['foldname']
    main_dir = os.path.dirname(os.path.dirname(foldname))
    
    run_status_file = main_dir + '\\generated\\run_status.txt'
    f = open(run_status_file, 'r')
    run_status = f.read()
    
    run_status_file_len = main_dir + '\\generated\\run_status_len.txt'
    f = open(run_status_file_len, 'r')
    run_status_len = f.read()
    
    print(run_status)
    return({'status': run_status,'status_count': run_status_len.replace('\n','')})
    
@app.route('/runFolder', methods=['POST'])
def runFolder():
    st_time = str(datetime.datetime.now())
    st_time = st_time.split('.')[0].replace(' ','').replace(':','').replace('-','')
    
    foldname = request.form['foldname']
    if foldname == foldName:
        foldname = foldname   
    else:
        foldname = foldname
        os.environ['foldname'] = foldname
        dotenv.set_key(dotenv_file, 'foldname', os.environ['foldname'], quote_mode='always')

    bridge = request.form['bridge']
    if bridge == bridgePath:
        bridge = bridge   
    else:
        bridge = bridge
        os.environ['bridge'] = bridge
        dotenv.set_key(dotenv_file, 'bridge', os.environ['bridge'], quote_mode='always')

    kafka = request.form['kafka']
    
    if kafka == kafkaPath:
        kafka = kafka   
    else:
        kafka = kafka
        os.environ['kafka'] = kafka
        dotenv.set_key(dotenv_file, 'kafka', os.environ['kafka'], quote_mode='always')
    
    materialize = request.form['materialize']
    if materialize == materializePath:
        materialize = materialize   
    else:
        materialize = materialize
        os.environ['materialize'] = materialize
        dotenv.set_key(dotenv_file, 'materialize', os.environ['materialize'], quote_mode='always')

    sandbox = request.form['sandbox']
    if sandbox == sandboxPath:
        sandbox = sandbox   
    else:
        sandbox = sandbox
        os.environ['sandbox'] = sandbox
        dotenv.set_key(dotenv_file, 'sandbox', os.environ['sandbox'], quote_mode='always')
    
        
    matip = request.form['matip']
    if matip == matIP:
        matip = matip
    else:
        matip = matip
        os.environ['matip'] = matip        
        dotenv.set_key(dotenv_file, "matip", os.environ["matip"], quote_mode='always')
    
    matport = request.form['matport']
    
    if matport == matPort:
        matport = matport
    else:
        matport = matport
        os.environ['matport'] = matport    
        dotenv.set_key(dotenv_file, "matport", os.environ["matport"], quote_mode='always')
        
    matuser = request.form['matuser']
    if matuser == matUser:
        matuser = matuser
    else:
        matuser = matuser
        
        os.environ['matuser'] = matuser    
        dotenv.set_key(dotenv_file, "matuser", os.environ["matuser"], quote_mode='always')
            
    matpass = request.form['matpass']
    if matpass == matPass:
            matpass = matpass
    else:
        matpass = matpass
        os.environ['matpass'] = matpass
        dotenv.set_key(dotenv_file, "matpass", os.environ["matpass"], quote_mode='always')
    
    matdb = request.form['matdb']
    if matdb == matDB:
            matdb = matdb
    else:
        matdb = matdb
        os.environ['matdb'] = matdb    
        dotenv.set_key(dotenv_file, "matdb", os.environ["matdb"], quote_mode='always')

    
    if not os.path.exists(bridge):
        os.makedirs(bridge)
    else:
        shutil.rmtree(bridge)
        os.makedirs(bridge)
    if not os.path.exists(kafka):
        os.makedirs(kafka)
    else:
        shutil.rmtree(kafka)
        os.makedirs(kafka)
    if not os.path.exists(materialize):
        os.makedirs(materialize)
    else:
        shutil.rmtree(materialize)
        os.makedirs(materialize)

    if not os.path.exists(sandbox):
        os.makedirs(sandbox)
    else:
        shutil.rmtree(sandbox)
        os.makedirs(sandbox)                
        
    if not os.path.exists(foldname):
        return({'status':'Please enter correct folder location'})
    else:
        xl_files=glob.iglob(foldname + '/**/*.csv', recursive=True)
        x_files = []
        for file in xl_files:
            x_files.append(file)
        
        main_dir = os.path.dirname(os.path.dirname(foldname))
        if not os.path.exists(main_dir + '\\generated\\'):
            os.makedirs(main_dir + '\\generated\\')
        
        run_status_file = main_dir + '\\generated\\run_status.txt'
        run_status_file_len = main_dir + '\\generated\\run_status_len.txt'
        
        f = open(run_status_file, 'w')
        f.write(' -- files run status -- \n')
        f.close()
        
        files_processed = 0
        
        for x_file in x_files:
        
            files_processed = files_processed + 1
            
            try:
                bridge_value = open("./templates/bridge_template.txt", "r")
                kafka_value = open("./templates/kafka_template.txt", "r")
                materialize_value = open("./templates/materialize_template.txt", "r")
                result = convert(file=x_file,run=0,matip=matip,matport=matport,matuser=matuser,matpass=matpass,matdb=matdb,
                                        foldname=foldname,bridge=bridge, materialize=materialize, kafka=kafka, sandbox=sandbox, 
                                    kafka_value=kafka_value.read(), materialize_value=materialize_value.read(), bridge_value=bridge_value.read())
                kafka_value.close()
                materialize_value.close()
                
                f = open(run_status_file, 'a')
                f.write(os.path.basename(x_file).upper() + ' completed\n')
                f.close()
                
            except Exception as e:
                print("error in api app",str(e))
                
                f = open(run_status_file, 'a')
                f.write(os.path.basename(x_file).upper() + ' error ' + str(e) + '\n')
                f.close()
            
            f_len = open(run_status_file_len, 'w')
            f_len.write(str(files_processed) + "/" + str(len(x_files)) + '\n')
            f_len.close()
                
        
        

        
        return({'status':'Processing Completed'})

@app.route('/uploadFile', methods=['POST'])
def uploadFile():
    f = request.files['file']
    files = request.files.getlist("file")
    st_time = str(datetime.datetime.now())
    st_time = st_time.split('.')[0].replace(' ','').replace(':','').replace('-','')
    
    try:
        for f in files:
            bridge = bridgePath
            materialize = materializePath
            sandbox = sandboxPath    
            kafka = kafkaPath
            
            if not os.path.exists(bridge):
                os.makedirs(bridge)
            else:
                shutil.rmtree(bridge)
                os.makedirs(bridge)
            if not os.path.exists(kafka):
                os.makedirs(kafka)
            else:
                shutil.rmtree(kafka)
                os.makedirs(kafka)
            if not os.path.exists(materialize):
                os.makedirs(materialize)
            else:
                shutil.rmtree(materialize)
                os.makedirs(materialize)

            if not os.path.exists(sandbox):
                os.makedirs(sandbox)
            else:
                shutil.rmtree(sandbox)
                os.makedirs(sandbox)                

            if not os.path.exists(app.config['UPLOAD_FOLDER']):
                os.makedirs(app.config['UPLOAD_FOLDER'])
            else:
                shutil.rmtree(app.config['UPLOAD_FOLDER'])
                os.makedirs(app.config['UPLOAD_FOLDER'])                
                
            
            user_val = request.args.get('user_val')
            fn = request.args.get('foldname')
            if fn != None: 
                if fn == foldName:
                    foldname = fn   
                else:
                    foldname = fn
                    os.environ['foldname'] = foldname
                    dotenv.set_key(dotenv_file, "foldname", os.environ["foldname"], quote_mode='always')
            else:
                foldname = None
            matip = request.args.get('matip')
            if matip == matIP:
                matip = matip
            else:
                matip = matip
                os.environ['matip'] = matip        
                dotenv.set_key(dotenv_file, "matip", os.environ["matip"], quote_mode='always')
            
            matport = request.args.get('matport')
            if matport == matPort:
                matport = matport
            else:
                matport = matport
                os.environ['matport'] = matport    
                dotenv.set_key(dotenv_file, "matport", os.environ["matport"], quote_mode='always')
                
            matuser = request.args.get('matuser')
            if matuser == matUser:
                matuser = matuser
            else:
                matuser = matuser
                os.environ['matuser'] = matuser    
                dotenv.set_key(dotenv_file, "matuser", os.environ["matuser"], quote_mode='always')
                    
            matpass = request.args.get('matpass')
            if matpass == matPass:
                    matpass = matpass
            else:
                matpass = matpass
                os.environ['matpass'] = matpass    
                dotenv.set_key(dotenv_file, "matpass", os.environ["matpass"], quote_mode='always')
            
            matdb = request.args.get('matdb')
            if matdb == matDB:
                    matdb = matdb
            else:
                matdb = matdb
                os.environ['matdb'] = matdb    
                dotenv.set_key(dotenv_file, "matdb", os.environ["matdb"], quote_mode='always')
                
            fname = secure_filename(f.filename)
            #fname = fname.split('.')[0] + '_' + st_time + '.' + fname.split('.')[1]
            
            f.save(os.path.join(app.config['UPLOAD_FOLDER'],fname))

            materialize_value = user_val
            bridge_value = open("./templates/bridge_template.txt", "r")
            result = convert(file=os.path.join(app.config['UPLOAD_FOLDER'],fname),run=1,matip=matip,matport=matport,matuser=matuser,matpass=matpass,matdb=matdb,foldname=foldname,
                                bridge=bridge, materialize=materialize, kafka=kafka, sandbox=sandbox, materialize_value=materialize_value, bridge_value=bridge_value.read())
            bridge_value.close()
        if len(files) == 1:
            return({'status':result[0],'status2':result[1]})
        else:
            return({'status':'Multiple Files Output Saved In Processed Folder','status2':'Multiple Files Output Saved In Processed Folder'})
            
    except Exception as e:
        return({'status':'Incorrect Or No File Provided','status2':'Incorrect Or No File Provided'})


#generate tables.yaml file dynamically via hasura URL for hasura-project
@app.route('/generateYaml', methods=['GET'])
def generate_yaml():
    print(hasuraYamlFile)
    print(shellScriptFile)
    try: 
        generate_yaml_file = convert_yaml(hasuraYamlFile)
        shell_script_path = r'start '+shellScriptFile + ' ' + hasuraCLIPath
        
        subprocess.Popen(shell_script_path, shell=True)
    except Exception as e:
        print(str(e)) 

    return ""



if __name__ == '__main__':
    app.run(host='localhost', port=8080, debug=True)
    
    
# @app.route('/favicon.ico')
# def favicon():
    # return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')
import os
import datetime
import glob
import dotenv
import shutil
from static.my_py.convert_to_sql import convert

base_dir = '.'
dotenv_file = dotenv.find_dotenv()
dotenv.load_dotenv(dotenv_file)
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

def runFolder():
    st_time = str(datetime.datetime.now())
    st_time = st_time.split('.')[0].replace(' ','').replace(':','').replace('-','')
    
    bridge = bridgePath
    materialize = materializePath
    sandbox = sandboxPath    
    kafka = kafkaPath
    foldname = foldName
    matip = matIP
    matport = matPort
    matuser = matUser
    matpass = matPass
    matdb = matDB
        
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
            
        try:
            for x_file in x_files:
                bridge_value = open("./templates/bridge_template.txt", "r")
                kafka_value = open("./templates/kafka_template.txt", "r")
                materialize_value = open("./templates/materialize_template.txt", "r")
                result = convert(file=x_file,run=0,matip=matip,matport=matport,matuser=matuser,matpass=matpass,matdb=matdb,
                                     foldname=foldname,bridge=bridge, materialize=materialize, kafka=kafka, sandbox=sandbox, 
                                    kafka_value=kafka_value.read(), materialize_value=materialize_value.read(), bridge_value=bridge_value.read())
                bridge_value.close()
                kafka_value.close()
                materialize_value.close()
            print("Daas Code Generator successfully generated files.")
        except Exception as e:
            print("error in app",str(e))
        
        return({'status':'Processing Completed'})

    
runFolder()
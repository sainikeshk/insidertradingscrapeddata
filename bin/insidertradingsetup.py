import os
import subprocess
import config_reader
loggin_user=os.getlogin()

config = config_reader.get_config()
binfolderpath = config.get("binfolder_path", None)
os.chdir(binfolderpath)
try:
	subprocess.call(['python3','config_reader.py'])
	# print('task 1 done')
	subprocess.call(['python3','setup.py'])
	subprocess.call(['python3','sql_connection.py'])
	subprocess.call(['python3','insidertradingDownload.py'])
except:
	subprocess.call(['python','config_reader.py'])
	subprocess.call(['python','setup.py'])
	subprocess.call(['python','sql_connection.py'])
	subprocess.call(['python','insidertradingDownload.py'])
# print('running')

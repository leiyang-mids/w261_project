from time import time, gmtime, strftime
from subprocess import call

logName = 's3://w261.data/HW13/criteo_search_log_' + strftime("%d%b%Y_%H%M%S", gmtime())
call(['aws', 's3', 'cp', '/home/hadoop/lei/Criteo_Driver_2.py', logName, '--region', 'us-west-2'])

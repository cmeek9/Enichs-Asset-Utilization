import configparser
from Modules.SEQLogging import SeqLog

# define config for connetion strings for various sources
config = configparser.ConfigParser(interpolation=None)
config.read('config.ini')

seq_url = config['Seq']['url']
seq_key = config['Seq']['key']

logging  = SeqLog(seq_url, seq_key)

pull_conxn_str = config['Database']['extrct_tbl']
load_conxn_str = config['Database']['load_tbl']
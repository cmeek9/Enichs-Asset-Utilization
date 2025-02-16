import os
from Modules.SEQLogging import SeqLog
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve values from environment variables
seq_url = os.getenv('SEQ_URL')
seq_key = os.getenv('SEQ_KEY')

logging = SeqLog(seq_url, seq_key)

pull_conxn_str = os.getenv('PULL_CONN_STR')
load_conxn_str = os.getenv('LOAD_CONN_STR')


pull_engine = create_engine(pull_conxn_str)
load_engine = create_engine(load_conxn_str)
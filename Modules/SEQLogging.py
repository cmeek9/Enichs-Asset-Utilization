import configparser
import requests
import json
from datetime import datetime
 
class SeqLog:
    def __init__(self, seq_url, seq_key) -> None:
        self.seq_url = seq_url
        self.seq_api_key = seq_key
        pass
 
    def info(self, message):
        self.send_message(message, "Information")
    def fatal(self, message):
        self.send_message(message, "Fatal")
    def error(self, message):
        self.send_message(message, "Error")
    def warning(self, message):
        self.send_message(message, "Warning")
    def send_message(self, message, level):
        formatted_message = {"@t": str(datetime.now().isoformat()), "@mt": message, "@l": level}
        json_message = json.dumps(formatted_message)
        print(json_message)
        try:
            r = requests.post(f"{self.seq_url}", data=json_message, headers={
                "ContentType": "application/vnd.serilog.clef",
                "X-Seq-ApiKey": self.seq_api_key
            })
            r.raise_for_status()
        except Exception as e:
            print(e)
            print("Error logging to Seq")
        pass
    def set_encoder(self, obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError(f"{obj} is not JSON serializable")
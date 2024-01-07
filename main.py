import pandas as pd
import os
import requests
import json
import traceback
import curlify

WEBHOOK_URL = "WEBHOOK_URL"

class ChatAlerting :

    def __init__(
        self,
        publish_report:bool
    ) -> None:
        
        self.publish_report = publish_report
        self.webhook_url = os.getenv(WEBHOOK_URL, "")
        self.file_path = "/Users/prasoonbalara/Downloads/cereal.csv"
        self.unique_manufacturers = {}
        self.headers = {
            "Content-type": "application/json"
        }
        self.max_rows_per_column = 20

    def get_row(
        self,
        col_values:list[str]
    ) -> str:
        result = ""
        for i in range(len(col_values)):
            val = col_values[i]
            result = result + val + (' ' * (self.max_rows_per_column - len(val)))
            if i != len(col_values) - 1:
                result = result + "|"
        result = result + '\n' + '-' * (2*self.max_rows_per_column + 1)
        return result


    def publish_report_to_chat(self):
        if not self.webhook_url:
            raise ValueError("No webhook url found")

        rows = []
        rows.append(self.get_row(["Manufacturer", "Count"]))
        for key in self.unique_manufacturers:
            val = str(self.unique_manufacturers.get(key))
            rows.append(self.get_row(
                [key, val]
            ))
        
        payload_string = "*Session Stats*\n" + "```" + "\n".join(rows) + "```"
        print(payload_string)

        body_data = {
            "text" : payload_string
        }
        request_body = json.dumps(body_data)

        response = requests.post(self.webhook_url, headers=self.headers, data=request_body)
        print(curlify.to_curl(response.request))

        if response.status_code != 200:
            raise ValueError("received non 200 status code while sending message via webhook")



    def run(self) :
        data = pd.read_csv(self.file_path)
        column_name = "mfr"

        for val in data[column_name]:
            if val in self.unique_manufacturers:
                curr_count = self.unique_manufacturers.get(val)
                self.unique_manufacturers.update({
                    val : curr_count+1
                })
                continue
            self.unique_manufacturers.update({
                val: 1
            })

        if self.publish_report:
            try :
                self.publish_report_to_chat()
            except Exception as e:
                print(traceback.print_exc())
        


if __name__ == '__main__':
    try :
        print(os.getenv(WEBHOOK_URL, ""))
        action = ChatAlerting(True)
        action.run()
    except Exception as e:
        print("Error while running alerting script", e)
    except KeyboardInterrupt:
        print("Exiting...")
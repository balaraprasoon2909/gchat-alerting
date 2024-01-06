import pandas as pd
import os
import click
import requests
import json

WEBHOOK_URL = "WEBHOOK_URL"

class ChatAlerting :

    def __init__(
        self,
        **kwargs
    ) -> None:
        
        self.row_limit = kwargs.get("row_limit", -1)
        self.publish_report = kwargs.get("publish_report", False)
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
            if i != len(col_values - 1):
                result = result + "|"
            result = result + '-' * (2*self.max_rows_per_column + 1)
        return result


    def publish_report(self):
        if not self.webhook_url:
            raise ValueError("No webhook url found")

        rows = []
        for key, val in self.unique_manufacturers:
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
                self.publish_report()
            except Exception as e:
                print(e)
        



@click.command()
@click.command(
    "--publish-report",
    is_flag=True,
    help="Send report to slack"
)
def cli(**kwargs):
    action = ChatAlerting(**kwargs)
    action.run()


if __name__ == '__main__':
    try :
        cli()
    except Exception as e:
        print("Error while running alerting script", e)
    except KeyboardInterrupt:
        print("Exiting...")
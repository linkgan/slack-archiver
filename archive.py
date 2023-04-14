import yaml
from gspread_pandas import Spread
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pandas as pd

from logger import get_logger
import os.path
import os
import json

logger = get_logger(__name__)


class Archive:
    """

    Go through my slack channels and archive them into Gsheets
    as well as keep a copy of the messages in an output folder

    """

    def __init__(self, configs):
        logger.info("Initializing Archive object")
        self.now = time.localtime()
        self.date = time.strftime("%Y-%m-%d", self.now)
        self.channels_to_archive = configs["channels_to_archive"]
        self.bot_token = configs["bot_token"]
        self.client = WebClient(token=self.bot_token)
        self.gspread = Spread(configs["GSHEET_NAME"])
        self.configs = configs

    def get_channels(self):
        self.channel_dict = {}

        results = self.client.conversations_list()
        channel_list = [{x["name"]: x["id"]} for x in results["channels"]]
        logger.info(channel_list)
        for c in channel_list:
            self.channel_dict.update(c)

        return self

    def _get_username(self, user: str) -> str:
        try:
            user_info = self.client.users_info(user=user).data
            username = user_info["user"]["name"]
            return username
        except SlackApiError as e:
            print("Error fetching user information: {}".format(e))

    def _to_file(self, messages, channel, type) -> None:
        dir = f"./output/{self.date}"
        if not os.path.exists(dir):
            os.makedirs(dir)
            logger.info("Directory created successfully")
        else:
            logger.warning("Directory already exists")

        file_path = dir + f"/{channel}_{type}.json"

        result = {
            "channel_name": channel,
            "channel_id": self.channel_dict[channel],
            "messages": messages,
        }
        # open the file in "write" mode

        logger.info(f"Writing file at: {file_path}")
        with open(file_path, "w") as file:
            # write the JSON data to the file
            json.dump(result, file, indent=4)

    def get_conversation_history(self, channel_id, channel_name):
        now = time.time()
        logger.info("Getting conversation history")

        df = pd.DataFrame()

        self.result = self.client.conversations_history(
            channel=channel_id,
        )

        messages = self.result["messages"]
        logger.info(f"Number of messages: {len(messages)}")

        self._to_file(messages, channel=channel_name, type="conversation_history")

        df = df.from_records(messages)

        df["readable_time"] = pd.to_datetime(df["ts"], unit="s")
        df["date"] = df["readable_time"].dt.strftime("%Y-%m-%d")
        df["archive_time"] = now
        df["username"] = df["user"].apply(lambda x: self._get_username(x))
        # df['date'] = df["ts"].apply(lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d"))

        self.df = df
        self.min_ts = df["ts"].min()
        self.max_ts = df["ts"].max()

        threads = self.get_threads(channel_id)
        self._to_file(threads, channel=channel_name, type="conversation_replies")

        return self

    def get_threads(self, channel_id) -> dict:
        # Get non-null rows with threads
        logger.info("Looking for threads...")
        try:
            threads = self.df[self.df["thread_ts"].notnull()][
                "thread_ts"
            ].values.tolist()
        except KeyError:
            logger.info("There were no threads in this channel")

        else:
            thread_list = []
            for t in threads:
                response = self.client.conversations_replies(channel=channel_id, ts=t)
                message = response["messages"]
                thread_list.append(message)
                # pprint.pprint(message)
            return thread_list

    def download_files(self):
        # TODO:
        return None

    def gsheets_io(self, channel: str):
        logger.info("Saving conversation history to gsheet for [{}]".format(channel))
        sheet_name = channel + "_new"

        self.gspread.df_to_sheet(
            self.df, index=False, sheet=sheet_name, start=self.configs['gsheet']['cell_start'], replace=True
        )
        self.gspread.update_cells("A1", "B1", ["Last updated on:", self.date])

    def merge_channel(self, channel: str)->None:
        logger.info("Merging old and new data: [{}]".format(channel))
        old_df = self.gspread.sheet_to_df(sheet=channel, start_row=3, index=None)
        #Check if old_df is empty
        if len(old_df) > 0:
            old_df = old_df[old_df['ts']<=self.min_ts]
        
        new_df = pd.concat([old_df, self.df])
        self.gspread.df_to_sheet(
            new_df, index=False, sheet=channel, start="A3", replace=True
        )
        self.gspread.update_cells("A1", "B1", ["Last merged on:", self.date])
        

    def full_run(self):
        """Run the full flow of Archiver"""

        self.get_channels()
        for channel in self.channels_to_archive:
          self.get_conversation_history(self.channel_dict[channel], channel)
          self.gsheets_io(channel)
          self.merge_channel(channel)


def load_config(filename: str = "config.yaml") -> dict:
    
    """
    	Looks for the file `config.yaml` to set up the configs for this archiver.
		For an templated example see `example_config.yaml`
    """
    if os.path.isfile(filename):
        with open(filename, "r") as file:
            configs = yaml.safe_load(file)

        return configs

    else:
        logger.error(
            "Config file path does not exist, please create a config file before running."
        )


def main():
    configs = load_config()
    a = Archive(configs)
    a.full_run()


if __name__ == "__main__":
    main()

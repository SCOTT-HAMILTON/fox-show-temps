from collections import defaultdict
import pandas as pd
from datetime import datetime, timezone, timedelta
from pprint import pprint
import boto3
import h5py
import json
import numpy as np
import os
import re
import requests
import shutil
import time

auth = json.load(open("auth.json", "r"))
sigfox_login = auth["sigfox"]["login"]
sigfox_pswd = auth["sigfox"]["password"]
sigfox_devid = auth["sigfox"]["deviceId"]
sigfox_endpoint = f"https://{sigfox_login}:{sigfox_pswd}@api.sigfox.com/v2"

s3_endpoint = "https://s3.filebase.com"
aws_access_key_id = auth["filebase-s3"]["accessKeyId"]
aws_secret_access_key = auth["filebase-s3"]["secretAccessKey"]
bucket_name = auth["filebase-s3"]["bucketName"]
s3_client = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

H5_DATASET_NAME = "lanloup_temps"
NP_DTYPE = [
    ("timestamp", np.ulonglong),
    ("data", "<S8"),
    ("seqNum", np.ulonglong),
    ("lqi", np.short),
]


def get_season(dt):
    # Function to get the season based on the month of the datetime object
    month = dt.month
    if 3 <= month <= 5:
        return "Printemps"
    elif 6 <= month <= 8:
        return "Été"
    elif 9 <= month <= 11:
        return "Automne"
    else:
        return "Hiver"


def classify_messages_by_season_year(message_list):
    # Dictionary to store messages grouped by "season-year"
    messages_by_season_year = defaultdict(list)
    for msg in message_list:
        date = msg[0]
        season = get_season(date)
        year = date.year
        season_year_key = f"{season}-{year}"
        messages_by_season_year[season_year_key].append(
            (int(date.timestamp()), *msg[1:])
        )

    # Sort messages in each group by date
    for key, messages in messages_by_season_year.items():
        messages_by_season_year[key] = sorted(messages, key=lambda x: x[0])

    return dict(messages_by_season_year)


def list_files_in_bucket():
    pattern = r"(Printemps|Été|Automne|Hiver)-\d{4}\.hdf5"
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        data = [obj["Key"] for obj in response["Contents"]]
        return list(filter(lambda f: re.fullmatch(pattern, f) != None, data))
    else:
        print("Failed to list files.")
        return None


def download_file_from_bucket(object_name, output_file_path):
    try:
        s3_client.download_file(bucket_name, object_name, output_file_path)
        print("File downloaded successfully.")
    except Exception as e:
        print("Failed to download the file:", e)


def make_clean_dir(dir_path):
    try:
        shutil.rmtree(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass
    try:
        os.makedirs(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass


def download_all_files():
    make_clean_dir("downloads")
    files = list_files_in_bucket()
    print(f"[LOG] bucket files: {files}")
    for f in files:
        path = f"downloads/{f}"
        download_file_from_bucket(f, path)
    return files


def read_hdf5_to_numpy(file_path, dataset_name):
    try:
        with h5py.File(file_path, "r") as hdf_file:
            dataset = hdf_file[dataset_name][:]
            return np.array(dataset)
    except Exception as e:
        print("Error reading the HDF5 file:", e)
        return None


def download_seasons_historic():
    downloaded_files = download_all_files()
    seasons_dict = dict()
    for f in downloaded_files:
        path = f"downloads/{f}"
        seasons_dict[f[:-5]] = read_hdf5_to_numpy(path, H5_DATASET_NAME)
    return seasons_dict


def get_int_temp(data):
    return int(data.hex()[-3:], 16) / 10.0


def get_ext_temp(data):
    return int(data.hex()[2:-3], 16) / 10.0


def timestamp_to_local(timestamp):
    locale_tz = datetime.now().astimezone().tzinfo
    return datetime.fromtimestamp(timestamp, locale_tz)


def add_breaking_lines(data_list, threshold):
    result = []
    last_datetime = None
    for item in data_list:
        current_datetime = item[0]
        if last_datetime is not None:
            time_diff = current_datetime - last_datetime
            if time_diff > threshold:
                # Insert a breaking line tuple with None values
                result.append(tuple(None for _ in item))
        result.append(item)
        last_datetime = current_datetime
    return result


seasons_historic = download_seasons_historic()
allmsgs = np.array(
    add_breaking_lines(
        list(
            map(
                lambda x: (
                    timestamp_to_local(x[0]),
                    get_int_temp(x[1]),
                    get_ext_temp(x[1]),
                    *x[1:],
                ),
                np.concatenate(list(seasons_historic.values())).tolist(),
            )
        ),
        timedelta(minutes=25),
    )
)

df = pd.DataFrame(allmsgs[:, 0:3], columns=["date", "internal", "external"])
print(f"[LOG] historic:\n{df}")

# Using graph_objects
import plotly.graph_objects as go

layout = go.Layout(
    title="Températures lanloup",
    xaxis={"title": "date"},
    yaxis={"title": "température (°C)"},
)
fig = go.Figure(
    [
        go.Scatter(x=df["date"], y=df["internal"], name="température intérieure"),
        go.Scatter(x=df["date"], y=df["external"], name="température extérieure"),
    ],
    layout=layout,
)
fig.show()

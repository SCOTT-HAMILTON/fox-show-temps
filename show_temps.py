from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pprint import pprint
import argparse
import boto3
import h5py
import json
import numpy as np
import os
import pandas as pd
import re
import requests
import shutil
import time

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


def list_files_in_bucket(s3_client):
    pattern = r"(Printemps|Été|Automne|Hiver)-\d{4}\.hdf5"
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        data = [obj["Key"] for obj in response["Contents"]]
        return list(filter(lambda f: re.fullmatch(pattern, f) != None, data))
    else:
        print("Failed to list files.")
        return None

def download_cid(cid, output_file_path):
    url = f"{ipfs_endpoint}/{cid}"
    with requests.get(url, stream=True) as r:
        with open(output_file_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

def download_file_from_bucket(object_name, output_file_path, s3_client):
    try:
        cid = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name,
        ).get("Metadata").get("cid")
        download_cid(cid, output_file_path)
        print(f"File {bucket_name}/{object_name} downloaded successfully.")
    except Exception as e:
        print(f"Failed to download the file {object_name}:", e)
        raise e


def make_clean_dir(dir_path):
    try:
        shutil.rmtree(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass
    try:
        os.makedirs(dir_path)
    except (FileExistsError, FileNotFoundError):
        pass


def download_all_files(s3_client):
    make_clean_dir("downloads")
    files = list_files_in_bucket(s3_client)
    print(f"[LOG] bucket files: {files}")
    for f in files:
        path = f"downloads/{f}"
        download_file_from_bucket(f, path, s3_client)
    return files


def read_hdf5_to_numpy(file_path, dataset_name):
    try:
        with h5py.File(file_path, "r") as hdf_file:
            dataset = hdf_file[dataset_name][:]
            return np.array(dataset)
    except Exception as e:
        print("Error reading the HDF5 file:", e)
        return None


def download_seasons_historic(s3_client):
    downloaded_files = download_all_files(s3_client)
    seasons_dict = dict()
    for f in downloaded_files:
        path = f"downloads/{f}"
        seasons_dict[f[:-5]] = read_hdf5_to_numpy(path, H5_DATASET_NAME)
    return seasons_dict


march_1_2025 = datetime(2025, 3, 1, tzinfo=timezone.utc) # data format switch date
Tmax, Tmin = 60.0, -60.0

def get_int_temp(data, timestamp):
    if datetime.fromtimestamp(timestamp, tz=timezone.utc) < march_1_2025:
        return int(data.hex()[-3:], 16) / 10.0
    else:
        return int(data.hex()[-3:], 16)*(Tmax-Tmin)/0xFFF+Tmin

def get_ext_temp(data, timestamp):
    if datetime.fromtimestamp(timestamp, tz=timezone.utc) < march_1_2025:
        return int(data.hex()[2:-3], 16) / 10.0
    else:
        return int(data.hex()[2:-3], 16)*(Tmax-Tmin)/0xFFF+Tmin

def get_batt_volt(data, timestamp):
    if datetime.fromtimestamp(timestamp, tz=timezone.utc) < march_1_2025:
        return int(data.hex()[:2], 16) * (3.3/256)*15/3.355
    else:
        return int(data.hex()[:2], 16) * 15/0xFF


def timestamp_to_local(timestamp):
    locale_tz = datetime.now().astimezone().tzinfo
    return datetime.fromtimestamp(timestamp, locale_tz)


def add_breaking_lines(data_list, threshold):
    result = []
    last_datetime = None
    for item in sorted(data_list, key=lambda x:x[0]):
        current_datetime = item[0]
        if last_datetime is not None:
            time_diff = current_datetime - last_datetime
            if time_diff > threshold:
                result.append(tuple(None for _ in item))
        result.append(item)
        last_datetime = current_datetime
    return result


parser = argparse.ArgumentParser(
    description="Python script to display collected temperatures from sigfox sensors"
)
parser.add_argument("--h5", type=str, help="Path to the hdf5 file to read")
args = parser.parse_args()
if args.h5:
    file_path = args.h5
    file_historic = {"_": read_hdf5_to_numpy(file_path, H5_DATASET_NAME)}
else:
    file_path = None
    auth = json.load(open("auth.json", "r"))
    s3_endpoint = auth["s3"]["endpoint"]
    aws_access_key_id = auth["s3"]["accessKeyId"]
    aws_secret_access_key = auth["s3"]["secretAccessKey"]
    bucket_name = auth["s3"]["bucketName"]
    ipfs_endpoint = auth["s3"]["ipfsEndpoint"]
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

seasons_historic = (
    download_seasons_historic(s3_client) if file_path is None else file_historic
)

allmsgs = np.array(
    add_breaking_lines(
        map(
            lambda x: (
                timestamp_to_local(x[0]),
                get_batt_volt(x[1], x[0]),
                get_int_temp(x[1], x[0]),
                get_ext_temp(x[1], x[0]),
                *x[1:],
            ),
            np.concatenate(list(seasons_historic.values())).tolist(),
        ),
        timedelta(minutes=25),
    )
)

df = pd.DataFrame(allmsgs[:, 0:4], columns=["date", "batt_volt", "internal", "external"])
print(f"[LOG] historic:\n{df}")

# Using graph_objects
import plotly.graph_objects as go

layout = go.Layout(
    title="Températures lanloup",
    xaxis={"title": "date"},
    yaxis={"title": "température (°C)"},
    yaxis2={
        "title": "tension batterie (V)",  # Label for the second y-axis
        "overlaying": "y",                # Overlay on the same x-axis
        "side": "right",                  # Place y-axis on the right
        "range": [-1, 28],                 # Define the range for the battery voltage
    },
)
fig = go.Figure(
    [
        go.Scatter(x=df["date"], y=df["internal"], name="température intérieure"),
        go.Scatter(x=df["date"], y=df["external"], name="température extérieure"),
        go.Scatter(x=df["date"], y=df["batt_volt"], name="tension batterie", yaxis="y2"),
    ],
    layout=layout,
)
fig.show()

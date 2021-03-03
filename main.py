import csv
import os
import shutil
import gzip
import pandas as pd
import json

ROOT_DIR = "/Volumes/Media/PhoneLab Dataset/logcat"


# "TABLE_NAME":"sqlite_sequence","SCHEMA":"cid:0,name:name,type:,notnull:0,dflt_value:null,pk:0...cid:1,name:seq,type:,notnull:0,dflt_value:null,pk:0...","Action":"SCHEMA"}

def main():
    # pd.set_option('display.max_colwidth', None)
    # pd.set_option('display.max_columns', None)

    for device_dir in next(os.walk(ROOT_DIR))[1]:
        # /tag/SQLite-Query-Phonelab/*/*/*.out.gz
        device_path = ROOT_DIR + "/" + device_dir + "/tag/SQLite-Query-Phonelab/2015/03/"
        # extract_all_in_dir(device_path)
        # read_sqlite_log(device_path)
    # print(devices_dir)

    df = read_sqlite_log("/Volumes/Media/PhoneLab Dataset/logcat/1b0676e5fb2d7ab82a2b76887c53e94cf0410826/tag/SQLite-Query-PhoneLab/2015/03/")
    # df = read_sqlite_log(
        # "/Volumes/Media/PhoneLab Dataset/logcat/0ee9cead2e2a3a58a316dc27571476e8973ff944/tag/SQLite-Query-PhoneLab/2015/03/")  # 1
    schema, query_log = parse_sqlite_log(df)
    export_to_csv(schema, query_log, "1b0676e5fb2d7ab82a2b76887c53e94cf0410826")


def export_to_csv(schema, query_log, device_dir):
    print("Exporting to CSV")
    if not os.path.exists(device_dir):
        os.makedirs(device_dir)
    if not os.path.exists(device_dir + "/data"):
        os.makedirs(device_dir + "/data")

    for k, v in schema.items():
        column_names = []
        if not os.path.exists(device_dir + "/data/" + k):
            os.makedirs(device_dir + "/data/" + k)
        with open(device_dir + "/data/" + k + "/" + k + ".csv", 'w') as out:
            csv_out = csv.writer(out, delimiter=';')
            splits = v.split("cid")
            for split in splits:
                subsplits = split.split(",")
                for subsubsplit in subsplits:
                    if "name:" in subsubsplit:
                        column_names.append(subsubsplit[5:])
            csv_out.writerow(column_names)

    with open(device_dir + "/query_log.csv", 'w') as out:
        csv_out = csv.writer(out, delimiter=';')
        csv_out.writerow(["start_timestamp", "end_timestamp", "date_time"])
        for row in query_log:
            csv_out.writerow(row)


def parse_sqlite_log(df):
    print("Parsing SQLite log")
    schema = {}
    query_log = []
    for row in df.itertuples(index=False):
        try:
            details = json.loads(row.details)
            if details["Action"] == "SCHEMA" and details["TABLE_NAME"] not in schema:
                schema[details["TABLE_NAME"]] = details["SCHEMA"]
            elif details["Action"] in ["SELECT", "INSERT", "UPSERT", "UPDATE",
                                       "DELETE"] and "PRAGMA" not in details["Results"]:
                sqlite_program = details["Results"]
                if sqlite_program.startswith('SQLiteProgram: '):
                    sqlite_program = sqlite_program[15:]
                query_log.append(
                    (row.start_timestamp, row.end_timestamp, row.date_time, sqlite_program))
        except:
            pass
    print(schema.keys())
    print(query_log)
    return schema, query_log


def read_sqlite_log(device_path):
    """
    Reads the sqlite logs and returns the concatenated dataframe per device, i.e. combines all days
    into one big ordered dataframe.
    """
    print("Reading SQLite log")
    files = sorted(
        [filename for filename in os.listdir(path=device_path) if filename.endswith(".out")],
        key=lambda x: int(os.path.splitext(x)[0]))
    df = pd.concat((pd.read_csv(device_path + filename, sep='\t', lineterminator='\n',
                                names=["device", "start_timestamp", "end_timestamp", "date_time",
                                       "unknown_1", "unknown_2", "unknown_3", "tag", "details"],
                                header=None) for filename in files), axis=0, ignore_index=True)
    return df


def extract_all_in_dir(path):
    """
    Extracts all the .gz files in a directory to that same directory if they are not already
    extracted
    """
    print(f"Extracting archives in {path}")
    for filename in os.listdir(path=path):
        if os.path.exists(path + os.path.splitext(filename)[0]):
            continue

        if filename.endswith(".gz"):
            with gzip.open(path + filename, 'rb') as f_in:
                with open(path + os.path.splitext(filename)[0], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


if __name__ == '__main__':
    main()

# Desired output:
# Per device, sequence of queries
# Per device, schema of the database

#!/usr/bin/env python3
"""
This script loads bulk data into SensorThings Databsase

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser

from mmm.common import human_readable_bytes
from mmm.metadata_collector import init_metadata_collector
from mmm import SensorThingsApiDB
import yaml
from mmm.core import load_fields_from_dict
import rich
import logging


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("--compress", help="Compress TimescaleDB chunks older than specified (e.g. 30days)",
                           type=str, required=False, default="")
    argparser.add_argument("--stats", help="Show space stats for PostgresQL", action="store_true")
    args = argparser.parse_args()
    
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    mc = init_metadata_collector(secrets)
    psql_conf = load_fields_from_dict(secrets["sensorthings"], ["database", "user", "host", "port", "password"])

    db = SensorThingsApiDB(psql_conf["host"], psql_conf["port"], psql_conf["database"], psql_conf["user"],
                                 psql_conf["password"], logging.getLogger(), timescaledb=True)

    if args.stats:
        obs_size = db.value_from_query('SELECT pg_total_relation_size(\'"OBSERVATIONS"\') AS table_size_bytes;')
        obs_count = db.value_from_query('SELECT count(*) from "OBSERVATIONS";')

        ts_count = db.value_from_query('SELECT count(*) from timeseries;')
        ts_size = db.value_from_query("SELECT hypertable_size('timeseries');")

        pr_count = db.value_from_query('SELECT count(*) from profiles;')
        pr_size = db.value_from_query("SELECT hypertable_size('profiles');")

        dt_count = db.value_from_query('SELECT count(*) from detections;')
        dt_size = db.value_from_query("SELECT hypertable_size('detections');")


        rich.print(f"OBSERVATIONS size {human_readable_bytes(obs_size)} for {obs_count} lines")
        rich.print(f"timeseries   size {human_readable_bytes(ts_size)} for {ts_count} lines")
        rich.print(f"profiles     size {human_readable_bytes(pr_size)} for {pr_count} lines")
        rich.print(f"detections     size {human_readable_bytes(dt_size)} MB for {dt_count} lines")

        before, after, ratio = db.timescale.compression_stats("timeseries")
        rich.print("Hypertable 'timeseries'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

        before, after, ratio = db.timescale.compression_stats("profiles")
        rich.print("Hypertable 'profiles'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

        before, after, ratio = db.timescale.compression_stats("detections")
        rich.print("Hypertable 'detections'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

    elif args.compress:
        rich.print("Compressing 'profiles'")
        db.timescale.compress_all("profiles", args.compress)
        before, after, ratio = db.timescale.compression_stats("profiles")
        rich.print("Hypertable 'profiles'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

        rich.print("Compressing 'timeseries'")
        db.timescale.compress_all("timeseries", args.compress)
        before, after, ratio = db.timescale.compression_stats("timeseries")
        rich.print("Hypertable 'timeseries'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

        rich.print("Compressing 'detections'")
        db.timescale.compress_all("detections", args.compress)
        before, after, ratio = db.timescale.compression_stats("detections")
        rich.print("Hypertable 'detections'")
        rich.print(f"    before compression: {human_readable_bytes(before)}")
        rich.print(f"     after compression: {human_readable_bytes(after)}")
        rich.print(f"     compression ratio: {ratio}")

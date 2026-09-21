#!/usr/bin/env python3
"""
Script that takes data from a SensorThings database and generates  NetCDF data

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 4/10/23
"""

from argparse import ArgumentParser
from mmm import DataCollector, setup_log, CkanClient
import yaml
import rich
import logging
import pandas as pd

from mmm.common import str_to_timerange
from mmm.metadata_collector import init_metadata_collector
import os

# Structure to store the generated resources. Some datasets may have dependencies

# TODO implement last (e.g. last month)
# TODO implement current (e.g. this month)


def generate_dataset(dataset_id: str, service_name: str, time_start: pd.Timestamp, time_end: pd.Timestamp, secrets,
                     log: logging.Logger, format:str= "", verbose=False, erddap_config=False, overwrite=False,
                     resources=[], local=False, publish=False, limit:int=0, no_files=False,
                     update_metadata=False) :
    """
    Generate a dataset following the configuration in the metadata database dataset register.
    :param dataset_id: id of the dataset register
    :param time_start:
    :param time_end:
    :param out_folder: output folder where the datasets will be generated. If null, use 'dataset_id' as folder name.
    :param secrets: secrets.yaml file
    :param secrets: For periodic datasets generate the current file, e.g. today's file or this monthly file
    :return: list of filenames
    """

    with open(secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    if not log:
        log = setup_log("gen_dataset", log_level="info")

    if verbose:
        log.setLevel(logging.DEBUG)

    mc = init_metadata_collector(secrets, log=log)
    dc = DataCollector(secrets, log, mc=mc)

    dc.generate_dataset(dataset_id, service_name, time_start, time_end, fmt=format, secrets=secrets, limit=limit,
                        overwrite=overwrite, erddap_config=erddap_config, resources=resources, local=local,
                        publish=publish, no_files=no_files, update_metadata=update_metadata)

def list_datasets(secrets, verbose=False):
    with open(secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    log = setup_log("sta_to_emso")
    mc = init_metadata_collector(secrets, log=log)
    datasets = mc.get_documents("datasets")
    for dataset in datasets:
        services = list(dataset["export"].keys())
        rich.print(f"'{dataset['#id']}' - services: {services}")


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("dataset_id", help="Dataset ID", type=str)
    argparser.add_argument("services", help="Service name (e.g. ERDDAP, CKAN, etc.)", nargs="+", type=str)
    argparser.add_argument("--list", help="List registered datasets and exit", action="store_true")
    argparser.add_argument("--limit", help="Limit data queries to N data points, for debugging only", type=int, default=0)
    argparser.add_argument("--local", help="Do not send to destination server", action="store_true")
    argparser.add_argument("--current", help="Generate current dataset, e.g. if monthly from start to end of the current month", action="store_true")
    argparser.add_argument("--last", help="Generate last dataset, e.g. if monthly from start to end of the previous month", action="store_true")
    argparser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    argparser.add_argument("-F", "--force", help="Overwrite any existing dataset", action="store_true")
    argparser.add_argument("-e", "--erddap", help="Configure dataset in erddap", action="store_true")
    argparser.add_argument("-r", "--resources", help="Generate only resources within list", type=str, nargs="+")
    argparser.add_argument("-o", "--overwrite", help="Overwrite existing datasets", action="store_true")
    argparser.add_argument("-u", "--update-metadata", help="Update the metadata (only implemented in Zenodo)", action="store_true")

    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False,
                           default="secrets.yaml")
    argparser.add_argument("-t", "--time-range", help="Time range with ISO notation, like 2022-01-01/2023-01-01",
                           type=str, required=False, default="")
    argparser.add_argument("-p", "--period", help="period to generate files, 'day', 'month' or 'year'. If not set a "
                                                  "single big file will be generated", type=str,
                           required=False, default="")

    argparser.add_argument("-f", "--format",type=str,  required=False, default="",
                           help="Suggest format such as netcdf, csv, etc. May not work for all datasets")

    argparser.add_argument("-P", "--publish", help="For Zenodo datasets, publish it and get the DOI", action="store_true")
    argparser.add_argument("--no-files", help="For Zenodo datasets, create the record but do not upload files", action="store_true")


    args = argparser.parse_args()

    deliver = True
    if args.local:
        deliver = False

    if args.time_range:
        tstart, tend = str_to_timerange(args.time_range)
    else:
        tstart = ""
        tend = ""

    log = setup_log("gen_dataset", log_level="info")
    if args.verbose:
        log.setLevel(logging.DEBUG)

    if args.current:
        raise ValueError("Unimplemented")

    if args.last:
        raise ValueError("Unimplemented")


    for service in args.services:
        generate_dataset(args.dataset_id, service, tstart, tend, args.secrets, log, format=args.format,
                         verbose=args.verbose, erddap_config=args.erddap, overwrite=args.overwrite,
                         resources=args.resources, local=args.local, publish=args.publish, limit=args.limit,
                         no_files=args.no_files, update_metadata=args.update_metadata)
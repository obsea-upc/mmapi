import logging
from datetime import datetime, date, timezone
from pathlib import Path
import time
from threading import Thread
from typing import Tuple, List, Optional
import markdown
import requests
import pandas as pd
import os
import json
import numpy as np

from emso_metadata_harmonizer.metadata.emso import init_emso_metadata
from mmm.common import LoggerSuperclass, assert_type, assert_types, get_linked_resource_conf, download_file, CYN, \
    human_readable_bytes, get_file_md5, extract_netcdf_metadata, check_sensor_deployments, run_over_ssh, GRN

from mmm.fileserver import FileServer, get_file
import rich


class GbifResource:
    def __init__(self, row: pd.Series, temp_folder="temp"):
        """
        Input dataframe should have the same columns
        :param row:
        """
        self.log = logging.getLogger()
        assert_type(row, pd.Series)

        self.dataset_id = row["dataset_id"]
        self.resource_id = row["resource_id"]
        self.service = row["service"]
        self.format = row["format"]
        self.data_from = row["data_from"]
        self.data_to = row["data_to"]
        self.path = row["path"]
        self.host = row["host"]
        self.url = row["url"]
        self.creation_date = row["creation_date"]
        self.modification_date = row["modification_date"]
        self.time_min = row["time_min"]
        self.time_max = row["time_max"]
        self.lat_min = row["lat_min"]
        self.lat_max = row["lat_max"]
        self.lon_min = row["lon_min"]
        self.lon_max = row["lon_max"]
        self.depth_min = row["depth_min"]
        self.depth_max = row["depth_max"]
        self.time_min = row["time_min"]
        self.md5 = row["md5"]

        self.doi = ""
        self.gbif_record = ""
        if row["doi"]:
            self.doi = row["doi"]
        if row["gbif_record"]:
            self.gbif_record = row["gbif_record"]

        self.local_file = ""
        self.temp_folder = temp_folder

        self.basename = os.path.basename(self.path)
        self.year = int(self.data_from.strftime("%Y")) # Year covering the dataset

    def __repr__(self):
        return f"GBIF Resource -> {self.dataset_id}:{self.resource_id}:{self.service}:{self.format}:{self.data_from.strftime('%Y-%m-%d')}"



class GbifClient(LoggerSuperclass):
    def __init__(self, dc: "DataCollector", secrets: dict, fileserver: FileServer, log):
        """
        Zenodo client

        Uses:
        - MetadataCollector for dataset / people / organization metadata
        - FileServer as source when local files are missing
        - Zenodo RDM API for create / update / version / upload / publish
        """
        LoggerSuperclass.__init__(self, log, "ZENODO", colour=GRN)
        assert_type(fileserver, FileServer)

        self.dc = dc
        self.mc = dc.mc
        self.secrets = secrets
        self.fileserver = fileserver

        self.url = secrets["gbif"]["url"]
        self.user = secrets["gbif"]["username"]
        self.password = secrets["gbif"]["password"]

        self.organization = secrets["gbif"]["organization"]


    def resolve_gbif_resource(self, dataset_conf: dict,  tstart: pd.Timestamp | None = None,
                                tend: pd.Timestamp | None = None, yearly=False) -> list:
        """
        Resolve the files that belong to one GBIF resource. GBIF should point to fileserver file already uploaded to Zenodo.

        Strategy:
        1. Match gbif.resource.id against fileserver.resource.id
        2. Try dataset_registry first
        3. Fallback to fileserver path scanning

        returns files (list), doi (str), gbif_record (str), linked_resource, source_service
        """
        assert_type(dataset_conf, dict)
        assert bool(tstart) == bool(tend), f"start and end time must be both set!"
        format_dwca = "dwca"
        target_service = "fileserver"

        dataset_id = dataset_conf["#id"]
        title = dataset_conf["export"]["zenodo"]["title"]

        query = f"""
            select *
            from {self.mc.dataset_registry_table}
            where
                LOWER(dataset_id) = LOWER('{dataset_id}')                                  
                and LOWER(service) = LOWER('{target_service}')   
                and LOWER(format) = '{format_dwca}';
        """

        if tstart and tend:
            ts = tstart.strftime("%Y-%m-%dT%H:%M:%S")
            te = tend.strftime("%Y-%m-%dT%H:%M:%S")
            query = query.replace(";", f"and data_from >= '{ts}' and data_to <= '{te}';")

        df = self.mc.db.dataframe_from_query(query)
        grs = [GbifResource(row, title) for _, row in df.iterrows()]

        resources_by_year = {}
        if yearly:
            self.info(f"Creating yearly gbif records")
            for gr in grs:
                if gr.year not in resources_by_year.keys():
                    resources_by_year[gr.year] = []
                resources_by_year[gr.year].append(gr)
            return list(resources_by_year.values())

        else:
            return grs


    def process_mmapi_dataset(self,
                              dataset_conf: dict,
                              tstart: pd.Timestamp|str="",
                              tend: pd.Timestamp|str="",
                              overwrite=False) -> list:


        assert_type(dataset_conf, dict)
        assert_types(tstart, [pd.Timestamp, str])
        assert_types(tend, [pd.Timestamp, str])
        assert "installation" in dataset_conf["export"]["gbif"].keys()

        gbif_resources = self.resolve_gbif_resource(dataset_conf, tstart, tend)

        for gr in gbif_resources:
            if not gr.doi:
                raise ValueError(f"Resource does not have a DOI! {gr}")
            self.upload_gbif_resource(dataset_conf, gr)


    def upload_gbif_resource(self,dataset_conf: dict, resource: GbifResource):

        session = requests.Session()
        session.auth = (self.user, self.password)
        session.headers["Accept"] = "application/json"
        self.info("Registering the dataset as HTTP link, GBIF will read the DwC content from: ")
        url = self.url
        payload = {
            "publishingOrganizationKey": self.organization,
            "installationKey": dataset_conf["export"]["gbif"]["installation"],
            "type": "SAMPLING_EVENT",  # core rowType is dwc:Event
            "title": dataset_conf["title"],
            "description": "The dataset is registered with minimal metadata, which is overwritten once GBIF can access the file.",
            "language": "eng",
            "license": "http://creativecommons.org/licenses/by/4.0/legalcode",  # must match eml.xml (CC-BY 4.0)
            "doi": resource.doi
        }
        resp = session.post(url + "/dataset", json=payload, timeout=60)  # json= serialises and sets Content-Type
        resp.raise_for_status()

        dataset_key = resp.json()  # response body is the UUID as a JSON string
        with open("gbif_test.txt", "w") as f:
            f.write(dataset_key)

        self.info(f"Dataset registered: {dataset_key}")
        self.dc.mc.update_gbif_record(resource.dataset_id,
                                      resource.resource_id,
                                      resource.service,
                                      [resource.data_from],
                                      [resource.data_to],
                                      str(dataset_key))

        self.info(f"Adding DwC-a as HTTP endpoint")
        resp = session.get(f"{self.url}/dataset/{dataset_key}/endpoint", timeout=60)
        resp.raise_for_status()

        if any(e["type"] == "DWC_ARCHIVE" and e["url"] == resource.url for e in resp.json()):
            self.info("Endpoint already set")
        else:
            endpoint = {"type": "DWC_ARCHIVE", "url": resource.url}
            resp = session.post(f"{self.url}/dataset/{dataset_key}/endpoint", json=endpoint, timeout=60)
            if not resp.ok:
                self.error(f"HTTP {resp.status_code}[/red]: {resp.text}")
                resp.raise_for_status()

            self.info(f"Endpoint added: {resp.text}")


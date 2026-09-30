#!/usr/bin/env python3
"""

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 27/5/24
"""

# TODO: Test AssertionError with different variable - units for the same dataset
# TODO: Make sure that FOI filter works by checking all "blue" data
# TODO: Make sure that time filter is working in datasets with period=none

import logging
import random
import shutil
import unittest
import os
import sys
import rich
from threading import Thread
import yaml
import time
import requests
import numpy as np
import pandas as pd
import json
import psycopg2
from PIL import Image, ImageDraw
import traceback
import dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (project root)
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

# Add the parent directory to the sys.path
sys.path.insert(0, parent_dir)

from mmm import (init_metadata_collector, setup_log, init_data_collector, propagate_metadata_to_sensorthings,
                 bulk_load_data, propagate_metadata_to_ckan, get_station_deployments)
from mmm.common import LoggerSuperclass, run_subprocess, file_list, dir_list, check_url, retrieve_url, \
    download_file, WHT
from mmapi import run_metadata_api
from sta_timeseries import run_sta_timeseries_api



redirect_stdout = False
test_status = []
test_log_files = []


log_level = logging.CRITICAL
logging.getLogger('matplotlib.font_manager').setLevel(logging.ERROR)
logging.getLogger('emso_metadata_harmonizer').setLevel(logging.ERROR)

def str_to_bool(value: str) -> bool:
    return value.strip().lower() == "true"

def get_json(url, params={}):
    r = requests.get(url, params=params)
    if r.status_code > 299:
        raise ConnectionError(f"HTTP error='{r.status_code}' at url={url}")
    return json.loads(r.text)


def post_json(url, data):
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, headers=headers, data=json.dumps(data), timeout=10)
    if r.status_code > 299:
        raise ConnectionError(f"HTTP error='{r.status_code}' at url={url}")


def patch_json(url, data):
    headers = {"Content-Type": "application/json"}
    r = requests.patch(url, headers=headers, data=json.dumps(data))
    if r.status_code > 299:
        raise ConnectionError(f"HTTP error='{r.status_code}' at url={url}")


class TestMMAPI(unittest.TestCase, LoggerSuperclass):
    @classmethod
    def setUpClass(cls):
        dotenv.load_dotenv("config-tests.env")
        global redirect_stdout
        redirect_stdout = str_to_bool(os.environ["REDIRECT_STDOUT"])

        # Process environment file
        cls.timeseries_data = str_to_bool(os.environ["TIMESERIES_DATA"])
        cls.profiles_data = str_to_bool(os.environ["PROFILES_DATA"])
        cls.detections_data = str_to_bool(os.environ["DETECTIONS_DATA"])
        cls.files_data = str_to_bool(os.environ["FILES_DATA"])
        cls.json_data = str_to_bool(os.environ["JSON_DATA"])

        cls.fileserver_test = str_to_bool(os.environ["TEST_FILESERVER"])
        cls.erddap_test = str_to_bool(os.environ["TEST_ERDDAP"])
        cls.ckan_test = str_to_bool(os.environ["TEST_CKAN"])

        log = setup_log("mmapi-test")
        LoggerSuperclass.__init__(cls, log, "test", colour=WHT)

        log_level = os.environ["LOG_LEVEL"].lower()
        if log_level == "debug":
            log.setLevel(logging.DEBUG)
        elif log_level == "info":
            log.setLevel(logging.INFO)
        elif log_level == "warning":
            log.setLevel(logging.WARNING)
        elif log_level == "error":
            log.setLevel(logging.ERROR)
        elif log_level == "critical":
            log.setLevel(logging.CRITICAL)

        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        logging.getLogger('flask').setLevel(logging.ERROR)

        cls.secrets = "secrets-test.yaml"
        with open(cls.secrets) as f:
            cls.conf = yaml.safe_load(f)["secrets"]


        cls.mmapi_url = cls.conf["mmapi"]["root_url"] + "/mmapi/v1.0"
        cls.sta_url = cls.conf["sensorthings"]["url"]

        with open("sta-timeseries.env") as f:
            for line in f.readlines():
                line = line.strip()
                if "STA_TS_ROOT_URL" in line:
                    cls.sta_ts_url = line.split("=")[1].replace("\"", "")
                    break

        os.makedirs("erddapData", mode=0o777, exist_ok=True)
        # Create volumes for all docker services
        with open("docker-compose.yaml") as f:
            docker_config = yaml.safe_load(f)

        cls.docker_volumes = []
        for name, service in docker_config["services"].items():
            for key, value in service.items():
                if key == "volumes":
                    for volume in value:
                        try:
                            src, dst = volume.split(":")
                        except ValueError:
                            src, dst, prm = volume.split(":")
                        if "." not in os.path.basename(src):  # if dot in name assume its a folder
                            os.makedirs(src, exist_ok=True, mode=0o777)

                        cls.docker_volumes.append(src)

        # Make sure that ERDDAP has a clean datasets.xml file
        shutil.copy2("volumes/conf/datasets.xml.default", "volumes/conf/datasets.xml" )

        log.info("Delete all files and folders in fileserver")
        folder = docker_config["services"]["fileserver"]["volumes"][0].split(":")[0]

        files = file_list(folder)
        for f in files:
            os.remove(f)
        dirs = dir_list(folder)
        dirs = sorted(dirs, reverse=True)
        for d in dirs:
            os.rmdir(d)

        log.info("Setting up containers with docker compose up... (this may take a while)")
        run_subprocess("docker compose up -d --build", fail_exit=True)

        os.makedirs("volumes/fileserver/pictures", exist_ok=True)

        # make sure that all servieces are up and running with at least a quick get
        urls = {
            "ERDDAP": "http://localhost:8090/erddap/index.html",
            "SensorThings": "http://localhost:8080/FROST-Server/v1.1/Sensors",
            "CKAN": "http://localhost:5001/api/3/"
        }
        timeout = 120
        tinit = time.time()
        for service, url in urls.items():
            code = 404
            log.info(f"Trying to reach service {service}...")
            while code > 300:
                try:
                    r = requests.get(url, timeout=5)
                    code = r.status_code
                except requests.exceptions.RequestException:
                    pass
                if time.time() - tinit > timeout:
                    log.error("[red]Timeout error!")
                    raise TimeoutError("Could not connect to service")

                if code < 300:
                    pass
                else:
                    time.sleep(2)

        log.info("Setup CkanClient")
        log.info("Let's do somethings quick and dirty to get the ckan_admin key")
        os.system("docker exec ckan-test ckan user token add ckan_admin tk1 2>/dev/null| tail -n 1 | sed 's/\t//g' >  ckan.key")
        # If success, we should have now the API key in the ckan.key file
        with open("ckan.key") as f:
            ckan_key = f.read().strip()

        if len(ckan_key) < 10:
            raise ValueError(f"CKAN TOKEN too short! '{ckan_key}'")
        cls.conf["ckan"]["api_key"] = ckan_key

        log.info("Setup Metadata Collector...")
        cls.mc = init_metadata_collector(cls.conf, log=log)
        cls.log = log
        log.info("Setup Data Collector...")
        cls.dc = init_data_collector(cls.conf, log, mc=cls.mc)
        cls.stadb = cls.dc.sta

        log.info("Clearing Metadata DB database...")
        cls.mc.drop_all()
        cls.dc.sta.drop_all()
        cls.ckan = cls.dc.ckan

    def test_01_launch_metadata_api(self):
        """Run all tests for MMAPI in a sequential manner"""
        
        self.log.info("Launching Metadata API in a dedicate thread...")
        #     run_flask_app(secrets, args.environment, log, mc, thread=True)

        sys.stdout = open(os.devnull, 'w')
        mapi = Thread(target=run_metadata_api, args=(self.secrets, self.log, self.mc), daemon=True)
        mapi.start()
        time.sleep(0.1)
        sys.stdout = sys.__stdout__
        d = get_json(self.mmapi_url)
        self.assertIsInstance(d, dict)

    def test_02_add_units_with_mc(self):
        """Adding ALL metadata documents via API and/or MetadataCollector"""
        
        self.log.info("Inserting several 'units' via API")
        docs = file_list("metadata/units")

        for filename in docs:
            with open(filename) as f:
                data = json.load(f)
                # Insert units via MC
            doc_id = data["#id"]
            self.info(f"Insert {doc_id}")
            a = self.mc.insert_document("units", data)
            self.assertIsInstance(a, dict)

            lvl = self.log.getEffectiveLevel()
            self.log.setLevel(logging.CRITICAL)

            with self.assertRaises(NameError) as cm:
                self.mc.insert_document("units", data)
            the_exception = cm.exception
            self.assertEqual(type(the_exception), NameError)

            self.log.setLevel(lvl)

            # Make wure we can access them via api
            url = self.mmapi_url + f"/units/{data['#id']}"
            units = get_json(url)
            self.assertEqual(data["symbol"], units["symbol"])
            # Make sure that update works


    def test_03_add_variables_with_api(self):
        """adding units and variables via API and check the history"""

        self.log.info("Inserting several 'variables' via API")
        for filename in file_list("metadata/variables"):
            with open(filename) as f:
                data = json.load(f)

            doc_id = data["#id"]
            original_desc = data["description"]

            post_json(self.mmapi_url + "/variables", data)

            data["description"] += " MODIFIED"
            modified_desc = data["description"]

            # Replace document with another name and make sure that we can still keep track of the verisons
            self.mc.replace_document("variables", doc_id, data)
            d1 = get_json(self.mmapi_url + f"/variables/{doc_id}/history/1")
            d2 = get_json(self.mmapi_url + f"/variables/{doc_id}/history/2")
            self.assertEqual(d1["description"], original_desc)
            self.assertEqual(d2["description"],  modified_desc)

            data["description"] = original_desc

            # Revert to first version using API
            patch_json(self.mmapi_url + f"/variables/{doc_id}", data)

    def test_04_insert_all_metadata(self):
        self.info("Load ALL documents from 'metadata' folder")
        collections = [
            "organizations",
            "people",
            "processes",
            "programmes",
            "projects",
            "qualityControl",
            "resources",
            "sensors",
            "stations",
            "activities",
            "operations",
            "datasets",
            # "units",  # already integrated in test 02
            #"variables" # already integrated in test 03
        ]
        for collection in collections:
            docs = file_list(os.path.join("metadata", collection))
            docs = [f for f in docs if f.endswith(".json")]
            docs = sorted(docs)
            for doc in docs:
                with open(doc) as f:
                    data = json.load(f)
                # Insert all documents
                try:
                    self.info(f"Inserting collection '{collection}' doc='{data['#id']}'")
                    self.mc.insert_document(collection, data)
                except Exception as e:
                    self.error(f"Error in document {doc}")
                    raise e


    def test_05_insert_wrong_metadata(self):
        self.info("Load WRONG documents from 'metadata' folder")
        collections = ["processes", "programmes", "projects", "qualityControl", "resources", "sensors", "stations",
                       "activities", "operations", "datasets", "units", "variables"]
        for collection in collections:
            folder = os.path.join("metadata", collection + ".errors")
            if not os.path.exists(folder):
                continue
            docs = file_list(folder)
            for doc in docs:
                with open(doc) as f:
                    data = json.load(f)
                # Insert all documents
            # supress logs
            lvl = self.log.getEffectiveLevel()
            self.log.setLevel(logging.CRITICAL)
            with self.assertRaises(ValueError):
                self.mc.insert_document(collection, data)
            self.log.setLevel(lvl)

    def test_06_metadata_checks(self):
        """Adding average process"""

        # Assess two OBSEA deployments
        self.assertEqual(len(get_station_deployments(self.mc, "OBSEA")), 2)



    def test_20_propagate_to_sensorthings(self):
        """Propagate metadata from Metadata DB to SensorThingsAPI"""
        
        propagate_metadata_to_sensorthings(self.dc, [], self.conf["sensorthings"]["url"], update=True)

        # Make sure that we have defaultFeatureOfInterest
        self.dc.sta.get_datastream_id("AWAC", "OBSEA", "CDIR", "profiles", "30min")


    def test_21_launch_sta_timeseries(self):
        """launching sensorthings timeseries API"""
        sys.stdout = open(os.devnull, 'w')
        mapi = Thread(target=run_sta_timeseries_api, args=["sta-timeseries.env", self.log, 8081], daemon=True)
        mapi.start()
        time.sleep(1)
        sys.stdout = sys.__stdout__
        timeout = 10
        tinit = time.time()
        while time.time() - tinit < timeout:
            try:
                d = get_json(self.sta_ts_url)
                self.assertIsInstance(d, dict)
                break
            except Exception as e:
                time.sleep(0.2)


    def test_30_ingest_avg_timeseries_data(self):
        """Ingesting average timeseries data using the API"""
        if not self.timeseries_data:
            self.skipTest("config skips timeseries data")
        # Generate sine wave values
        frequency = 3
        dates = pd.date_range(start='2022-01-01T00:00:00', end="2022-01-01T23:59:59", freq='30min')
        tvector = np.arange(0, len(dates)) / len(dates)
        # Create a pandas DataFrame
        df = pd.DataFrame({
            'timestamp': dates,
            "TEMP": np.sin(2 * np.pi * frequency * tvector),
            "CNDC": np.cos(2 * np.pi * frequency * tvector + np.pi/2)
        })
        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        sta = self.dc.sta
        sta.initialize_dicts()  # update dicts
        temp_id = sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "timeseries", average="30min")
        cndc_id = sta.get_datastream_id("SBE37", "OBSEA", "CNDC", "timeseries", average="30min")

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)

        # Assert that we get an error with wrong data types
        with self.assertRaises(AssertionError):
            sta.get_datastream_id("SBE37", "OBSEA", "CNDC", "banana")

        self.log.setLevel(lvl)

        foi_id = sta.value_from_query('select "ID" from "FEATURES" limit 1;')

        self.info("Injecting 100 first rows via FROST API...")
        for indx, row in df[:100].iterrows():
            obs = {
                "phenomenonTime": row["timestamp"],
                "result": row["TEMP"],
                "resultQuality": {"qc_flag": 1},
                "FeatureOfInterest": {"@iot.id": foi_id}
            }

            url = self.sta_url + f"/Datastreams({temp_id})/Observations"
            post_json(url, obs)
            obs = {
                "phenomenonTime": row["timestamp"],
                "result": row["CNDC"],
                "resultQuality": {"qc_flag": 1},
                "FeatureOfInterest": {"@iot.id": foi_id}
            }
            url = self.sta_url + f"/Datastreams({cndc_id})/Observations"
            post_json(url, obs)

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        # We should not able to insert it more than once
        with self.assertRaises(ConnectionError):
            post_json(url, obs)

        self.log.setLevel(lvl)

        self.log.info("Assert value is the same as injected via FROST")
        d = get_json(self.sta_url + f"/Datastreams({temp_id})/Observations",
                     params={"$top": "1", "$orderBy": "phenomenonTime asc"})
        self.assertAlmostEqual(d["value"][0]["result"], df["TEMP"].values[0])

        self.log.info("Get FROST averaged data using /Observations endpoint with filter...")
        data = get_json(self.sta_url + f"/Observations", params={"$filter": f"Datastream/id eq {temp_id}"})
        results = data["value"]
        self.assertEqual(len(results), len(df))

        self.log.info("Get FROST averaged data using /Datastream(xx)/Observations endpoint")
        data = get_json(self.sta_url + f"/Datastreams({temp_id})/Observations", params={"$top": 1000})
        results = data["value"]
        self.assertEqual(len(results), len(df))

        # self.log.info("Get STA-Timeseries averaged data using /Observations endpoint with filter...")
        # data = get_json(self.sta_ts_url + f"/Observations", params={"$filter": f"Datastream/id eq {temp_id}"})
        # results = data["value"]
        # self.assertEqual(len(results), len(df))
        #
        # self.log.info("Get STA-Timeseries averaged data using /Datastream(xx)/Observations endpoint")
        # data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations", params={"$top": 1000})
        # results = data["value"]
        # self.assertEqual(len(results), len(df))

        self.dc.sta.check_data_integrity()

    def test_31_bulk_load_raw_timeseries_data(self):
        """Ingesting average timeseries data using the API"""
        
        if not self.timeseries_data:
            self.skipTest("config skips timeseries data")

        sta = self.dc.sta

        def create_fake_data(start: str, end: str, freq: str, variables: list):

            # Generate sine wave values
            frequency = 3
            dates = pd.date_range(start=start, end=end, freq=freq)
            tvector = np.arange(0, len(dates)) / len(dates)
            # Create a pandas DataFrame
            df = pd.DataFrame({
                'timestamp': dates,
                "TEMP": np.sin(2 * np.pi * frequency * tvector),
                "CNDC": np.cos(2 * np.pi * frequency * tvector)
            })
            df["TEMP_QC"] = 1
            df["CNDC_QC"] = 1
            df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            return df


        df = create_fake_data("2023-01-01", "2023-03-31", "100s", ["TEMP", "CNDC"])
        filename = "test31.csv"
        df.to_csv(filename, index=False)
        bulk_load_data(filename, self.conf, "SBE37", "timeseries", "OBSEA", tmp_folder="./temp")

        self.info("Now, let's get the data and check that it's the same")
        temp_id = sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "timeseries")
        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$top=1000000")
        results = data["value"]
        self.assertEqual(len(results), len(df))
        self.dc.sta.check_data_integrity()

        self.info("Let's make sure that we have an exception when try to load 2 times the same data")
        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        with self.assertRaises(psycopg2.errors.UniqueViolation):
            bulk_load_data(filename, self.conf, "SBE37", "timeseries", "OBSEA", tmp_folder="./temp")
        self.log.setLevel(lvl)
        self.info("Let's delete some data and try to reload the gaps with missing-data")

        sta.exec_query(f"delete from timeseries where timestamp between '2023-02-01T00:00:00Z' and "
                       f"'2023-02-28T00:00:00Z';", fetch=False)

        bulk_load_data(filename, self.conf, "SBE37", "timeseries", "OBSEA",
                       tmp_folder="./temp", missing_data="direct")

        self.info("Now, let's get the data and check that it's the same")
        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$top=1000000")

        # after all this, the number of rows should be the same
        self.assertEqual(len(df), len(data["value"]))

        self.info("Now, try to fill data gaps with hourly data")
        sta.exec_query(f"""
            delete from timeseries where 
            timestamp between '2023-02-01T00:00:00Z' and '2023-02-02T00:00:00Z'            
            ;""", fetch=False)

        # Create data with another frequency, then try to insert it. If missing_data option is used, we should only
        # inject data in the empty period.
        df2 = create_fake_data("2023-01-01", "2023-03-31", "30min", ["TEMP", "CNDC"])
        filename = "test31.csv"
        df2.to_csv(filename, index=False)

        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$top=1000000")
        rows_before = len(data["value"])

        bulk_load_data(filename, self.conf, "SBE37", "timeseries", "OBSEA",
                       tmp_folder="./temp", missing_data="hourly")


        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$top=1000000")
        self.assertEqual(rows_before + 48, len(data["value"]))  # 24*2 points per hour, we should have 48 more rows

        self.info("Adding SBE16 data with partial overlap")

        os.remove(filename)



    def test_32_get_raw_timeseries_data_api(self):
        """get timeseries from the API"""
        if not self.timeseries_data:
            self.skipTest("config skips timeseries data")

        temp_id = self.dc.sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "timeseries")
        url = self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$orderBy=phenomenonTime asc&$top=1"
        data = get_json(url)
        first_value = data["value"][0]["result"]
        # Temperature first value is 0
        self.assertAlmostEqual(first_value, 0.0)
        self.dc.sta.check_data_integrity()

    def test_33_bulk_load_avg_timeseries_data(self):
        """Bulk load average timeseries data"""

        if not self.timeseries_data:
            self.skipTest("config skips timeseries data")
        # Generate sine wave values
        frequency = 1

        tstart = "2022-01-02T00:00:00Z"
        tend = "2022-04-01T00:00:00Z"

        dates = pd.date_range(start=tstart, end=tend, freq='30min')
        self.info(f"Bulk loading a LOT of averaged data ({len(dates)} points)")
        tvector = np.arange(0, len(dates)) / len(dates)
        # Create a pandas DataFrame
        df = pd.DataFrame({
            'timestamp': dates,
            "TEMP": np.sin(2 * np.pi * frequency * tvector),
            "CNDC": np.cos(2 * np.pi * frequency * tvector)
        })
        df["TEMP_QC"] = 1
        df["CNDC_QC"] = 1

        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        sta = self.dc.sta
        sta.initialize_dicts()  # update dicts
        filename = "test33.csv"
        df.to_csv(filename, index=False)

        start = pd.Timestamp(df["timestamp"].min())
        end = pd.Timestamp(df["timestamp"].max())

        deployments = self.mc.get_sensor_deployments("SBE37")

        deployments = self.mc.get_sensor_deployments("SBE37", interval=(start, end))

        bulk_load_data(filename, self.conf, "SBE37", "timeseries", "OBSEA",
                       tmp_folder="./temp", average="30min")
        os.remove(filename)
        self.info("Now, let's get the data and check that it's the same")
        temp_id = sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "timeseries", average="30min")
        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations",
                        params={
                            "$top": "10000000",
                            "$filter": f"resultTime ge {tstart} and resultTime le {tend}"
                        })

        results = data["value"]
        self.assertEqual(len(results), len(tvector))
        self.dc.sta.check_data_integrity()

    def test_40_ingest_avg_profile_data(self):
        """Ingesting average timeseries data using the API"""
        # Generate sine wave values
        if not self.profiles_data:
            self.skipTest("skip avg profile")

        frequency = 1
        dates = pd.date_range(start='2023-01-01', end="2023-01-02", freq='12h')
        depths = np.arange(0, 10)
        tvector = np.arange(0, len(dates)) / 1000
        data = {
            "timestamp": [],
            "depth": [],
            "CSPD": [],
            "CDIR": [],
            "UCUR": [],
            "VCUR": []
        }
        for date, t in zip(dates, tvector):
            for depth in depths:
                data["timestamp"].append(date)
                data["depth"].append(depth)
                for var in ["CSPD", "CDIR", "UCUR", "VCUR"]:
                    data[var].append(np.sin(2 * np.pi * frequency * t))

        # Create a pandas DataFrame
        df = pd.DataFrame(data)
        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        sta = self.dc.sta
        foi_id = sta.value_from_query('select "ID" from "FEATURES" limit 1;')
        sta.initialize_dicts()  # update dicts
        for var in ["CSPD", "CDIR", "UCUR", "VCUR"]:
            datastream_id = sta.get_datastream_id("AWAC", "OBSEA", var, "profiles", average="30min")
            for indx, row in df.iterrows():
                obs = {
                    "phenomenonTime": row["timestamp"],
                    "result": row[var],
                    "resultQuality": {"qc_flag": 1},
                    "FeatureOfInterest": {"@iot.id": foi_id},
                    "parameters": {"depth": row["depth"]}
                }
                url = self.sta_url + f"/Datastreams({datastream_id})/Observations"
                post_json(url, obs)
        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        # We should not able to insert it more than once
        with self.assertRaises(ConnectionError):
            post_json(url, obs)
        self.log.setLevel(lvl)
        self.dc.sta.check_data_integrity()

    def test_41_ingest_raw_profile_data(self):
        """Ingesting raw profiles data using the API"""
        if not self.profiles_data:
            self.skipTest("skip raw profile")

        # Generate sine wave values
        frequency = 1
        dates = pd.date_range(start='2023-01-01', end="2023-01-02", freq='30min')
        depths = np.arange(0, 10)
        tvector = np.arange(0, len(dates)) / 1000
        data = {
            "timestamp": [],
            "depth": [],
            "CSPD": [],
            "CDIR": [],
            "UCUR": [],
            "VCUR": []
        }
        variables = ["CSPD", "CDIR", "UCUR", "VCUR"]
        for date, t in zip(dates, tvector):
            for depth in depths:
                data["timestamp"].append(date)
                data["depth"].append(depth)
                for var in variables:
                    data[var].append(np.sin(2 * np.pi * frequency * t))

        # Create a pandas DataFrame
        df = pd.DataFrame(data)
        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Add Quality Control
        for var in variables:
            df[var + "_QC"] = 1

        foi_id = self.dc.sta.value_from_query('select "ID" from "FEATURES" limit 1;')

        filename = "test41.csv"
        df.to_csv(filename)

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        with self.assertRaises(AssertionError):
            bulk_load_data(filename, self.conf, "AWAC", "banana", "OBSEA", tmp_folder="./temp")
        self.log.setLevel(lvl)

        # Now use the correct data type
        bulk_load_data(filename, self.conf, "AWAC", "profiles", "OBSEA", tmp_folder="./temp")
        os.remove(filename)

        # Now, let's download all the data that we injected, see if it's available
        self.log.info(f"Get profile data from Datastreams/Observations")
        cdir_id = self.dc.sta.get_datastream_id("AWAC", "OBSEA", "CDIR", "profiles")
        data = get_json(self.sta_ts_url + f"/Datastreams({cdir_id})/Observations?$top=100000")
        results = data["value"]
        self.assertEqual(len(results), len(df))

        # Now, let's get the same data, but to general Observations endpoint and filtering by datastream
        self.log.info(f"Get profile data from Observations with filter")
        data = get_json(self.sta_ts_url + f"/Observations?$top=100000&$filter=Datastream/id eq {cdir_id}")
        results = data["value"]
        self.assertEqual(len(results), len(df))
        self.dc.sta.check_data_integrity()

    def test_42_add_profile_to_ctd(self):
        """Add profile data to a sensor that has both timeseries and profile data"""
        if not self.profiles_data:
            self.skipTest("skip avg profile")

        frequency = 1
        dates = pd.date_range(start='2023-01-01', end="2023-01-02", freq='30min')
        depths = np.arange(0, 10)
        tvector = np.arange(0, len(dates)) / 1000
        data = {
            "timestamp": [],
            "depth": [],
            "TEMP": [],
            "CNDC": []
        }
        variables = ["TEMP", "CNDC"]
        for date, t in zip(dates, tvector):
            for depth in depths:
                data["timestamp"].append(date)
                data["depth"].append(depth)
                for var in variables:
                    data[var].append(np.sin(2 * np.pi * frequency * t))

        # Create a pandas DataFrame
        df = pd.DataFrame(data)
        df["timestamp"] = df["timestamp"].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Add Quality Control
        for var in variables:
            df[var + "_QC"] = 1

        foi_id = self.dc.sta.value_from_query('select "ID" from "FEATURES" limit 1;')

        filename = "test42.csv"
        df.to_csv(filename)

        # Now use the correct data type
        bulk_load_data(filename, self.conf, "SBE37", "profiles", "OBSEA",
                       tmp_folder="./temp")
        os.remove(filename)

        # Now, let's download all the data that we injected, see if it's available
        self.log.info(f"Get profile data from Datastreams/Observations")
        temp_id = self.dc.sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "profiles")
        data = get_json(self.sta_ts_url + f"/Datastreams({temp_id})/Observations?$top=100000")
        results = data["value"]
        self.assertEqual(len(results), len(df))

        # Now, let's get the same data, but to general Observations endpoint and filtering by datastream
        self.log.info(f"Get profile data from Observations with filter")
        data = get_json(self.sta_ts_url + f"/Observations?$top=100000&$filter=Datastream/id eq {temp_id}")
        results = data["value"]
        self.assertEqual(len(results), len(df))
        self.dc.sta.check_data_integrity()

    def test_50_ingest_pics(self):
        """Create and ingest picture data"""
        if not self.files_data:
            self.skipTest("skip files")

        width, height = 2000, 2000  # Define the dimensions of the image

        self.log.info("Creating fake pictures...")
        files = []
        for i in range(1, 10):
            image = Image.new('RGB', (width, height), 'white')  # Create a white background image
            draw = ImageDraw.Draw(image)
            top_left = (50 + i * 10, 50 + i * 10)  # Top-left corner of the rectangle
            bottom_right = (150 + i * 10, 150 + i * 10)  # Bottom-right corner of the rectangle
            rectangle_color = 'green'  # Color of the rectangle
            draw.rectangle([top_left, bottom_right], fill=rectangle_color)
            f = f'rectangle_green_{i:02d}.png'
            image.save(f)  # Save the image to a file
            files.append(f)

        self.log.info("Creating Observations with the pictures...")
        dates = pd.date_range(start='2023-01-01', end="2023-01-02", freq='30min')
        fileserver = self.dc.fileserver
        datastream_id = self.dc.sta.get_datastream_id("IPC608", "OBSEA", "underwater_photography", "files")
        for i in range(len(files)):
            file = files[i]
            path = fileserver.send_file(f"./volumes/fileserver/pictures/IPC608", file)
            foi_id = self.dc.sta.value_from_query('select "ID" from "FEATURES" limit 1;')
            d = {
                "phenomenonTime": dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'),
                "resultTime": dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'),
                "result": path,
                "FeatureOfInterest": {"@iot.id": foi_id}
            }
            url = self.sta_url + f"/Datastreams({datastream_id})/Observations"
            post_json(url, d)

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        with self.assertRaises(ConnectionError):
            post_json(url, d)
        self.log.setLevel(lvl)

        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_url + f"/Datastreams({datastream_id})/Observations")
        results = data["value"]
        self.assertEqual(len(results), len(files))

        # Now download all files
        for result in results:
            retrieve_url(result["result"], output="image.png", timeout=1, attempts=1)
            os.remove("image.png")

        # Remove created files
        for file in files:
            os.remove(file)

    def test_51_ingest_pics_inference_detections(self):
        """Creating picture for bulk load pictures, inferences and detections"""
        if not self.files_data:
            self.skipTest("skip files")

        width, height = 2000, 2000  # Define the dimensions of the image

        # Create RED pictures
        pictures = []
        fois = []
        for i in range(1, 100):
            image = Image.new('RGB', (width, height), 'white')  # Create a white background image
            draw = ImageDraw.Draw(image)
            top_left = (50 + i * 10, 50 + i * 10)  # Top-left corner of the rectangle
            bottom_right = (150 + i * 10, 150 + i * 10)  # Bottom-right corner of the rectangle
            rectangle_color = 'red'  # Color of the rectangle
            draw.rectangle([top_left, bottom_right], fill=rectangle_color)
            filename = f"rectangle_red_{i:03d}.jpg"
            image.save(filename)  # Save the image to a file
            pictures.append(filename)
            fois.append("OBSEA_Biotop_Red")

        # Create BLUE pictures
        for i in range(1, 100):
            image = Image.new('RGB', (width, height), 'white')  # Create a white background image
            draw = ImageDraw.Draw(image)
            top_left = (50 + i * 10, 50 + i * 10)  # Top-left corner of the rectangle
            bottom_right = (150 + i * 10, 150 + i * 10)  # Bottom-right corner of the rectangle
            rectangle_color = 'blue'  # Color of the rectangle
            draw.rectangle([top_left, bottom_right], fill=rectangle_color)
            filename = f"rectangle_blue_{i:03d}.png"
            image.save(filename)  # Save the image to a file
            pictures.append(filename)
            fois.append("OBSEA_Biotop_Blue")

        # Let's create a csv file for file bulk load
        data = {
            "timestamp": [],
            "results": [],
            "datastream_id": [],
            "foi_id": []
        }
        start = "2023-02-01T00:00:00Z"
        end = "2023-03-01T00:00:00Z"
        dates = pd.date_range(start=start, end=end, freq='30min')
        datastream_id = self.dc.sta.get_datastream_id("IPC608", "OBSEA", "underwater_photography", "files")

        foi_dict = self.dc.sta.dict_from_query('select "NAME", "ID" from "FEATURES"')

        for i, (pic, foi_name) in enumerate(zip(pictures, fois)):
            url = self.dc.fileserver.send_file("./volumes/fileserver/pictures/IPC608", pictures[i])
            data["timestamp"].append(dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'))
            data["results"].append(url)
            data["datastream_id"].append(datastream_id)
            foi_id = foi_dict[foi_name]
            data["foi_id"].append(foi_id)
            os.remove(pictures[i])

        df = pd.DataFrame(data)
        datafile = "test51-files.csv"
        df.to_csv(datafile)

        # Now, bulk load it!
        bulk_load_data(datafile, self.conf, "IPC608", "files", "OBSEA",
                       tmp_folder="./temp")
        os.remove(datafile)

        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_url + f"/Datastreams({datastream_id})/Observations?$filter=phenomenonTime ge {start}&$top=1000")
        results = data["value"]
        self.assertEqual(len(results), len(pictures))
        # Now download all files
        for result in results:
           retrieve_url(result["result"])

        # Now create inference data
        # Let's create a csv file for file bulk load
        data = {
            "timestamp": [],
            "results": [],
            "parameters": [],
            "datastream_id": [],
            "foi_id": []
        }
        dates = pd.date_range(start='2023-02-01', end="2023-03-01", freq='30min')
        datastream_id = self.dc.sta.get_datastream_id("IPC608", "OBSEA", "FATX", "json")
        foi_id = self.dc.sta.value_from_query('select "ID" from "FEATURES" limit 1;')

        def random_fish_detections():
            _taxa = ["Chromis chromis", "Diplodus vulgaris"]

            def r():  # Random from 0 to 1
                return round(random.uniform(0, 1), 3)
            fdata = []
            for _ in range(0, random.randint(0, 4)):
                fdata.append({
                    "taxa": random.choice(_taxa),
                    "confidence": r(),
                    "bounding_box_xyxy": [r(), r(), r(), r()]
                 })
            return fdata

        for i in range(len(pictures)):
            data["timestamp"].append(dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'))
            data["results"].append(random_fish_detections())
            data["parameters"].append({"sourceImage": f"http://fake.url/{pictures[i]}"})
            data["datastream_id"].append(datastream_id)
            data["foi_id"].append(foi_id)
        df = pd.DataFrame(data)
        datafile = "test51-inference.csv"
        df.to_csv(datafile, index=False)

        bulk_load_data(datafile, self.conf, "IPC608", "json", "OBSEA", tmp_folder="./temp")
        os.remove(datafile)

        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_url + f"/Datastreams({datastream_id})/Observations?$filter=phenomenonTime ge {start}&$top=1000")
        results = data["value"]
        self.assertEqual(len(results), len(pictures))

        # Now, let's create some of detections
        data = {
            "timestamp": [],
            "results": [],
            "datastream_id": [],
            "foi_id": []
        }
        dates = pd.date_range(start='2023-02-01', end="2023-03-01", freq='30min')
        chromis_id = self.dc.sta.get_datastream_id("IPC608", "OBSEA", "chromis_chromis", "detections")
        diplodus_id = self.dc.sta.get_datastream_id("IPC608", "OBSEA", "diplodus_vulgaris", "detections")
        foi_id = self.dc.sta.value_from_query('select "ID" from "FEATURES" limit 1;')
        for i in range(len(pictures)):
            # chromis
            data["timestamp"].append(dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'))
            data["results"].append(4)
            data["datastream_id"].append(chromis_id)
            data["foi_id"].append(foi_id)

            # diplodus
            data["timestamp"].append(dates[i].strftime('%Y-%m-%dT%H:%M:%SZ'))
            data["results"].append(2)
            data["datastream_id"].append(diplodus_id)
            data["foi_id"].append(foi_id)

        df = pd.DataFrame(data)
        datafile = "test51-detections.csv"
        df.to_csv(datafile)
        bulk_load_data(datafile, self.conf, "IPC608", "detections", "OBSEA",
                       tmp_folder="./temp")
        os.remove(datafile)

        # Now, let's download all the data that we injected, see if it's available
        data = get_json(self.sta_ts_url + f"/Datastreams({chromis_id})/Observations?$filter=phenomenonTime ge {start}&$top=1000")
        results = data["value"]
        self.assertEqual(len(results), len(pictures))

        # Now, let's download all the data that we injected, see if it's available (only via sta-timeseries API)
        data = get_json(self.sta_ts_url + f"/Datastreams({diplodus_id})/Observations?$filter=phenomenonTime ge {start}&$top=1000")
        results = data["value"]
        self.assertEqual(len(results), len(pictures))

    def test_60_no_full_data_in_observations(self):
        sta = self.dc.sta
        sta.initialize_dicts()
        # Make sure that we do not have any fullData timeseries/profiles/detections in OBSERVATIONS
        self.log.info("Making sure that we don't have any timeseries, profiles or detections in the OBSERVATIONS table")
        wrong_datastreams = sta.timescale.check_data_in_observations()
        self.assertEqual(len(wrong_datastreams), 0)

        self.log.info("Force timeseries into OBSERVATIONS to get an exception")
        datastream_id = sta.get_datastream_id("SBE37", "OBSEA", "TEMP", "timeseries")
        foi_id = sta.datastream_properties[datastream_id]["defaultFeatureOfInterest"]
        d = {
            "phenomenonTime": "2000-01-01T00:00:00Z",
            "result": 3.14,
            "Datastream": {"@iot.id": datastream_id},
            "FeatureOfInterest": {"@iot.id": foi_id}
        }
        post_json(self.sta_url + "/Observations", d)

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        with self.assertRaises(ValueError):
            sta.timescale.check_data_in_observations(raise_exception=True)
        self.log.setLevel(lvl)

        wrong_datastreams = sta.timescale.check_data_in_observations(raise_exception=False)
        self.log.setLevel(lvl)
        self.assertEqual(len(wrong_datastreams), 1)

    def test_61_correct_data_in_hypertables(self):
        """check that only the correct data is stored in the hypertables"""
        if not (self.timeseries_data or self.profiles_data or self.detections_data):
            self.skipTest("Skipping hypertables")

        sta = self.dc.sta
        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)

        errors = sta.timescale.check_data_in_hypertables()
        self.assertEqual(len(errors), 0)

        # Now let's force some errors and catch the exceptions

        timeseries_id = sta.value_from_query(
            'select "ID" from "DATASTREAMS" where '
            '   "PROPERTIES"->>\'dataType\' = \'timeseries\''
            '   and ("PROPERTIES"->>\'fullData\')::boolean = true'
            '   limit 1;'
        )

        profile_id = sta.value_from_query(
            'select "ID" from "DATASTREAMS" where '
            '   "PROPERTIES"->>\'dataType\' = \'profiles\''
            '   and ("PROPERTIES"->>\'fullData\')::boolean = true'
            '   limit 1;'
        )

        detections_id = sta.value_from_query(
            'select "ID" from "DATASTREAMS" where '
            '   "PROPERTIES"->>\'dataType\' = \'detections\''
            '   and ("PROPERTIES"->>\'fullData\')::boolean = true'
            '   limit 1;'
        )

        # Adding 3 wrong elements
        sta.timescale.insert_to_timeseries("2020-01-01T00:00:00z", 3.14, 1, profile_id)
        sta.timescale.insert_to_detections("2020-01-01T00:00:00z", 15, timeseries_id)
        sta.timescale.insert_to_profiles("2020-01-01T00:00:00z", 13.01, 3.14, 1, detections_id)
        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        with self.assertRaises(ValueError):
            sta.timescale.check_data_in_hypertables()
        self.log.setLevel(lvl)

        wrong_ids = sta.timescale.check_data_in_hypertables(raise_exception=False)

        self.log.setLevel(lvl)

        self.assertEqual(len(wrong_ids), 3)
        self.assertIn(timeseries_id, wrong_ids)
        self.assertIn(profile_id, wrong_ids)
        self.assertIn(detections_id, wrong_ids)

        self.log.info("Deleting wrong observations...")
        self.dc.sta.exec_query(f"delete from timeseries where datastream_id = {profile_id};", fetch=False)
        self.dc.sta.exec_query(f"delete from detections where datastream_id = {timeseries_id};", fetch=False)
        self.dc.sta.exec_query(f"delete from profiles where datastream_id = {detections_id};", fetch=False)

    def test_70_fileserver_dataset_timeseries(self):
        """Creating a dataset"""
        os.makedirs("datasets", exist_ok=True)
        if not self.fileserver_test or not self.timeseries_data:
            self.skipTest("skip fileserver")

        # Export datasets with the default format (NetCDF)
        nc_datasets = self.dc.generate_dataset("obsea_ctd_full", "fileserver", overwrite=True)
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))
        # delete one dataset and ensure that we get an error when accessing it
        file_path = self.dc.fileserver.url2path(nc_datasets[0].url)
        os.remove(file_path)
        self.assertFalse(check_url(nc_datasets[0].url))

        # overwrite datasets
        nc_datasets = self.dc.generate_dataset("obsea_ctd_full", "fileserver", overwrite=True)
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        # Export as CSV datasets
        csv_datasets = self.dc.generate_dataset("obsea_ctd_full", "fileserver", overwrite=True, fmt="csv")
        for csv_dataset in csv_datasets:
            self.assertTrue(check_url(csv_dataset.url))

        # Export NetCDF
        nc_datasets = self.dc.generate_dataset("obsea_ctd_30min", "fileserver", overwrite=True)
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        # Export CSV
        nc_datasets = self.dc.generate_dataset("obsea_ctd_30min", "fileserver", overwrite=True, fmt="csv")
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        self.info("Make sure that we have a ValueError when trying to merge units")

        lvl = self.log.getEffectiveLevel()
        self.log.setLevel(logging.CRITICAL)
        # Force error in format
        with self.assertRaises(AssertionError):
            self.dc.generate_dataset("obsea_ctd_full", "erddap", "2020-01-01", "2030-02-01", fmt="potato")
            self.dc.generate_dataset("ctd_different_units", "fileserver", overwrite=True)
        self.log.setLevel(lvl)


    def test_72_fileserver_dataset_profiles(self):
        """Creating a dataset"""

        if not self.fileserver_test or not self.profiles_data:
            self.skipTest("skip")

        nc_datasets = self.dc.generate_dataset("awac_full", "fileserver", overwrite=True)
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        nc_datasets = self.dc.generate_dataset("awac_30min", "fileserver", overwrite=True)
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        nc_datasets = self.dc.generate_dataset("awac_full", "fileserver", overwrite=True, fmt="csv")
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

        nc_datasets = self.dc.generate_dataset("awac_30min", "fileserver", overwrite=True, fmt="csv")
        for nc_dataset in nc_datasets:
            self.assertTrue(check_url(nc_dataset.url))

    def test_74_fileserver_dataset_files(self):
        """Creating a dataset"""
        if not self.files_data:
            self.skipTest("skip files")

        zip_datasets = self.dc.generate_dataset("IPC608_pics", "fileserver", "2023-01-01", "2024-01-01")
        zip_datasets_blue = self.dc.generate_dataset("IPC608_pics_blue", "fileserver", "2023-01-01", "2024-01-01")
        zip_datasets_red = self.dc.generate_dataset("IPC608_pics_red", "fileserver", "2023-01-01", "2024-02-01")

        zips = zip_datasets + zip_datasets_blue + zip_datasets_red

        for zip_dataset in zips:
            if not zip_dataset:
                continue
            self.assertTrue(check_url(zip_dataset.url))

        dwca_dataset = self.dc.generate_dataset("biodiversity_datasets", "fileserver", "2020-01-01", "2020-02-01")
        for ds in dwca_dataset:
            self.assertTrue(check_url(ds.url))


    def test_80_propagate_to_ckan(self):
        if not self.ckan_test:
            self.skipTest("skip ckan")
        propagate_metadata_to_ckan(self.mc, self.ckan, self.log, collections=[])

    def test_81_generate_ckan_datasets(self):
        if not self.ckan_test:
            self.skipTest("skip ckan")
        self.dc.generate_dataset("obsea_ctd_full", "ckan", "2020-01-01", "2021-02-01") # default format
        self.dc.generate_dataset("obsea_ctd_full", "ckan", "2020-01-01", "2021-02-01", fmt="csv") # froce csv
        self.dc.generate_dataset("obsea_ctd_30min", "ckan", "2020-01-01", "2021-02-01")
        self.dc.generate_dataset("IPC608_pics", "ckan", "2023-01-01", "2024-02-01")

    def test_90_config_erddap(self):
        """creates a dataset and upload it to ERDDAP"""
        if not self.erddap_test:
            self.skipTest("skip erddap")
        nc_datasets = self.dc.generate_dataset("obsea_ctd_full", "erddap", "2020-01-01", "2020-02-01")
        self.info(f"Got {len(nc_datasets)} datasets")
        for nc_dataset in nc_datasets:
            # Convert from host path to erddap container path, otherwise ERDDAP will not see the files
            data_path = nc_dataset.exporter.path.replace("./datasets", "/datasets")
            nc_dataset.configure_erddap("volumes/conf/datasets.xml", "/datasets/obsea_ctd_full")
            nc_dataset.reload_erddap_dataset("erddapData")
            timeout = 10
            self.info(f"Wait {timeout} seconds for ERDDAP to reload...")

            dataset_info_url = "http://localhost:8090/erddap/info/" + nc_dataset.erddap_dataset_id + "/index.json"
            tinit = time.time()
            erddap_dataset_timeout = 10
            while not check_url(dataset_info_url):
                if (time.time() - tinit) > erddap_dataset_timeout:
                    raise ValueError(f"ERDDAP did not load {nc_dataset.erddap_dataset_id}")
                self.info(f"Waiting for ERDDAP to load {dataset_info_url}...")
                time.sleep(0.1)

            # Now get ERDDAP data!
            erddap_dataset = "mydataset.csv"
            dataset_url = "http://localhost:8090/erddap/tabledap/" + nc_dataset.erddap_dataset_id + ".csv"
            self.info(f"Downloading dataset from erddap: {dataset_url}")
            download_file(dataset_url, erddap_dataset)
            df = pd.read_csv(erddap_dataset)

    def test_91_config_erddap_with_daily_data(self):
        if not self.erddap_test:
            self.skipTest("skip erddap")
        """creates a dataset with daily files and upload it to ERDDAP"""
        nc_datasets = self.dc.generate_dataset("obsea_ctd_30min", "erddap", "2022-01-01", "2022-02-01")
        nc_dataset = nc_datasets[0]
        # Convert from host path to erddap container path, otherwise ERDDAP will not see the files
        data_path = nc_dataset.exporter.path.replace("./datasets", "/datasets")
        nc_dataset.configure_erddap("volumes/conf/datasets.xml", data_path)
        nc_dataset.reload_erddap_dataset("volumes/erddapData")

        # Now get ERDDAP data!
        erddap_dataset = "mydataset.csv"
        dataset_url = "http://localhost:8090/erddap/tabledap/" + nc_dataset.erddap_dataset_id + ".csv"
        self.info(f"Downloading dataset from erddap: {dataset_url}")
        retrieve_url(dataset_url, output=erddap_dataset)
        df = pd.read_csv(erddap_dataset)

        datasets = self.dc.generate_dataset("obsea_ctd_30min", "erddap", "2022-01-01", "2022-01-01")


    @classmethod
    def tearDownClass(cls):
        print_test_results()
        for f in test_log_files:
            os.remove(f)
        # input("press key to remove docker volumes...")
        # cls.log.info("stopping containers")
        # run_subprocess("docker compose down")
        # rich.print("Deleting temporal docker volumes...")
        # for volume in cls.docker_volumes:
        #     if os.path.isfile(volume):
        #         continue  # ignore volume files
        #
        #     for f in file_list(volume):
        #         os.remove(f)
        #
        #     # now remove any subdirs
        #     subdir_list = dir_list(volume)
        #     subdir_list = list(reversed(sorted(subdir_list)))
        #     for subdir in subdir_list:
        #         if os.path.isfile(subdir):
        #             raise ValueError("This should not happen!")
        #         os.rmdir(subdir)
        #
        # for volume in cls.docker_volumes:
        #     if os.path.isdir(volume):
        #         os.rmdir(volume)

def expand_str(s: str, width: int = 40, fill: str = '.') -> str:
    return s[:width].ljust(width, fill)

def print_test_results():
    rich.print("\n========== Test Summary =========")
    for entry in test_status:
        test_name, status, elapsed_time = entry
        status_lower = status.lower()
        if "error" in status_lower or "fail" in status_lower:
            row_style = "red"
        elif "skipped" in status_lower:
            row_style = "yellow"
        else:
            row_style = "green"
        status = status.ljust(12, " ")
        time_str = f"{elapsed_time:.0f} ms".rjust(10, " ")
        rich.print(f"[white]{test_name} [{row_style}] {status} [grey42]{time_str}")


class VerboseTestResult(unittest.TestResult):
    def startTest(self, test):
        super().startTest(test)
        self._start_time = time.monotonic()
        self._log_file = f"log/.{test._testMethodName}.log"
        self._log_fd = open(self._log_file, 'w')
        test_log_files.append(self._log_file)
        test_name = expand_str(test._testMethodName)
        if redirect_stdout:
            sys.stdout = self._log_fd
            sys.stderr = self._log_fd

    def stopTest(self, test):
        if redirect_stdout:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
        self._log_fd.close()

    def _restore_stdout(self):
        if redirect_stdout:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            self._log_fd.flush()

    def _elapsed(self):
        return time.monotonic() - self._start_time

    def _show_log(self):
        with open(self._log_file, 'r') as f:
            content = f.read()
        if content:
            rich.print(f"\n[grey42]--- captured output ---")
            print(content)
            rich.print(f"[grey42]--- end of output ---\n")

    def addSuccess(self, test):
        super().addSuccess(test)
        self._restore_stdout()
        test_name = expand_str(test._testMethodName)
        elapsed_time = 1000*(self._elapsed())
        rich.print(f"[white]Test  {test_name} [green] success ✅️ [grey42]({elapsed_time:.0f} ms)")
        test_status.append([test_name, "🟢 success", elapsed_time])

    def addError(self, test, err):
        super().addError(test, err)
        self._restore_stdout()
        rich.print(test)
        test_name = expand_str(test._testMethodName)
        rich.print(f"[white]Test  {test_name} [red] error ❌ [grey42]({1000*(self._elapsed()):.0f} ms)")
        self._show_log()
        test_status.append([test_name, "⛔ error", self._elapsed()])
        rich.print(f"[red]------------- traceback ---------------")
        rich.print(traceback.format_exc())
        rich.print(f"[red]---------------------------------------")
        input("error catched")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self._restore_stdout()
        test_name = expand_str(test._testMethodName)
        rich.print(f"[white]Test  {test_name} [red] failed ✗ [grey42]{1000*(self._elapsed()):.0f} ms")
        self._show_log()
        test_status.append([test_name, "🔴 failure", self._elapsed()])
        rich.print(f"[red]------------- traceback ---------------")
        rich.print(traceback.format_exc())
        rich.print(f"[red]---------------------------------------")
        input("error catched")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self._restore_stdout()
        test_name = expand_str(test._testMethodName)
        rich.print(f"[white]Test  {test_name} [yellow] skipped ⚠️  [grey42]({reason})")
        test_status.append([test_name, "🟡 skipped", self._elapsed()])


class VerboseTestRunner(unittest.TextTestRunner):
    resultclass = VerboseTestResult
    def run(self, test):
        print("\nRunning tests...\n")
        start = time.monotonic()
        result = super().run(test)
        elapsed = time.monotonic() - start
        print(f"\nFinished in {elapsed:.3f}s — "
              f"{result.testsRun} tests, "
              f"{len(result.failures)} failures, "
              f"{len(result.errors)} errors\n")
        return result

if __name__ == "__main__":
    init = time.time()
    unittest.main(
        failfast=True,
        testRunner=VerboseTestRunner(verbosity=0),
        verbosity=1)

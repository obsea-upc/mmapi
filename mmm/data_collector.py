#!/usr/bin/env python3
"""
This file implements the DataCollector, a class implementing generic data access and delivery.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 30/11/22
"""
import datetime
import logging
import socket
from typing import Tuple, List
import uuid
import yaml
import time
import emso_metadata_harmonizer.metadata
import numpy as np
import pandas as pd
import emso_metadata_harmonizer as emh
import os
import rich

from .darwin_core import DarwinCoreArchive
from .data_sources import SensorThingsApiDB
from .ckan import CkanClient
from .common import check_url, run_over_ssh, LoggerSuperclass, assert_types, GRN, RST, assert_type, populate_dict, \
    human_readable_bytes
from .data_sources.postgresql import sql_list
from .data_manipulation import merge_dataframes_by_columns, merge_dataframes, calculate_time_intervals, pivot_dataframe, \
    df_netcdf_normalization, floor_timestamp, ceil_timestamp, ensure_timestamp, find_first
from .gbif import GbifClient
from .metadata_collector import MetadataCollector, init_metadata_collector
from .fileserver import FileServer
from mmm.dataset import DatasetObject, DatasetBoundaries
from mmm.schemas import dataset_exporter_formats, valid_dataset_services, mmapi_data_types
from .zenodo import ZenodoClient


def init_data_collector(secrets: dict, log: logging.Logger, mc: MetadataCollector = None,
                        sta: SensorThingsApiDB = None):
    return DataCollector(secrets, log, mc=mc, sta=sta)


class DataCollector(LoggerSuperclass):
    """
    This class implements all methods to collect data from Databases and FileSystems, generate datasets and
    deliver them to the proper service.
    """

    def __init__(self, secrets: dict, log, mc: MetadataCollector = None, sta: SensorThingsApiDB = None):
        self.log = log
        LoggerSuperclass.__init__(self, log, "DC")
        if not mc:
            self.mc = init_metadata_collector(secrets, log=log)
        else:
            self.mc = mc

        if not sta:
            staconf = secrets["sensorthings"]
            self.sta = SensorThingsApiDB(staconf["host"], staconf["port"], staconf["database"], staconf["user"],
                                         staconf["password"], log, timescaledb=True)
        else:
            self.sta = sta
        self.fileserver = FileServer(secrets["fileserver"], log)

        try:
            self.ckan = CkanClient(mc, secrets["ckan"]["url"], secrets["ckan"]["api_key"], self.fileserver, log)
        except KeyError:
            self.warning("Could not initialize CKAN")
            self.ckan = None

        self.emso = None  # by default, do not initialize emso metadata

        try:
            self.zenodo = ZenodoClient(self, secrets, self.fileserver, log)
        except KeyError as e:
            self.warning(f"Could not initialize Zenodo: {e.__repr__()}")
            self.zenodo = None

        try:
            self.gbif = GbifClient(self, secrets, self.fileserver, log)
        except KeyError as e:
            self.warning(f"Could not initialize GbifClient: {e.__repr__()}")
            self.gbif = None

        self.boundaries = None

        self.erddap_url = secrets["erddap"].get("url", None)
        if not self.erddap_url:
            self.error("Cannot parse erddap url from settings!", exception=ValueError)

    def dataset_filename(self, dataset: dict, fmt: str, tstart: pd.Timestamp, tend: pd.Timestamp,
                         tmp_folder="temp") -> str:
        """
        Based on the configuration, generate the dataset filename
        """
        assert type(dataset) is dict
        assert type(tstart) is pd.Timestamp
        assert type(tend) is pd.Timestamp

        # convert from dataset_exporter_formats to real extensions
        extensions = {
            "netcdf": ".nc",
            "csv": ".csv",
            "zip": ".zip",
            "dwca": ".zip"
        }

        dataset_id = dataset["#id"]
        os.makedirs(tmp_folder, exist_ok=True)
        filename = dataset_id

        if tstart > pd.Timestamp("1900", tz="utc") and tstart.strftime("%Y%m%d") not in filename:
            filename +=  "_" + tstart.strftime("%Y%m%d") + "_" + tend.strftime("%Y%m%d")
        filename += extensions[fmt]
        return os.path.join(tmp_folder, filename)


    def resolve_time_coverage_db(self, conf:dict, resource:dict) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """
        Fetch the default time range from the database.

        Returns:
            A (time_start, time_end) tuple of pd.Timestamps from the database.
        """
        data_type = resource["dataType"]
        average = ""
        if "averagePeriod" in resource.keys():
            average = resource["averagePeriod"]

        sensor_ids = conf["@sensors"]
        platform_ids = conf["@stations"]

        # Get fieldOfView constraint if exists
        try:
            foi = conf["constraints"]["fieldOfView"]["@programmes"]
        except KeyError:
            foi = ""

        if data_type in ["files", "json"] or average:
            # files, JSONs and averages are always on "OBSERVATIONS"
            sensor_ids = conf["@sensors"]
            platform_ids = conf["@stations"]
            query = f"""
                select min("PHENOMENON_TIME_START") as time_start, max("PHENOMENON_TIME_START") as time_end
                from "OBSERVATIONS"
                where
                    "DATASTREAM_ID" in (
                        select "ID"
                        from "DATASTREAMS"
                        where
                            "PROPERTIES"->>'dataType' = '{data_type}'
                            --average 
                            and "SENSOR_ID" in (select "ID" from "SENSORS" where "NAME" in {sql_list(sensor_ids)})
                            and "THING_ID" in (select "ID" from "THINGS" where "NAME" in {sql_list(platform_ids)})
                    )                    
                ;"""

            if average:
                query = query.replace("--average",f""" and "PROPERTIES"->>'averagePeriod' = '{average}' """)

            if foi:
                query = query.replace(";", f""" and "FEATURE_ID" = (select "ID" from "FEATURES" where "NAME" = '{foi}');  """)


        else: # query directly timeseries / profiles / detections table
            query = f"""
                select min(timestamp) as time_start, max(timestamp) as time_end
                from {data_type}
                where
                    datastream_id in (
                        select "ID"
                        from "DATASTREAMS"
                        where
                            "PROPERTIES"->>'dataType' = '{data_type}'
                            and ("PROPERTIES"->>'fullData')::boolean = true
                            and "SENSOR_ID" in (select "ID" from "SENSORS" where "NAME" in {sql_list(sensor_ids)})
                            and "THING_ID" in (select "ID" from "THINGS" where "NAME" in {sql_list(platform_ids)})
                    )                    
                ;"""
        self.info("Getting dataset time range from database...")
        results = self.sta.list_from_query(query)
        return pd.Timestamp(results[0][0]), pd.Timestamp(results[0][1])


    def resolve_time_coverage(self,conf: dict, resource: dict, user_start: pd.Timestamp = None,
                           user_end: pd.Timestamp = None) -> Tuple[pd.Timestamp, pd.Timestamp]:
        """
        Resolve time_start and time_end using the following priority:
            1. User input
            2. Config constraint (tstart / tend from config) — lowest priority baseline.
            3. If neither user inputs nor config constraint exist, fall back to the database.
        """

        # Access time constraint from dataset config
        try:
            const_start, const_end = conf["constraints"]["timeRange"].split("/")
            const_start = pd.Timestamp(const_start)
            const_end = pd.Timestamp(const_end)
            if not const_start.tzinfo:
                const_start = pd.Timestamp.tz_localize(const_start, "utc")
            if not const_end.tzinfo:
                const_end = pd.Timestamp.tz_localize(const_end, "utc")

            self.debug(f"Dataset timeRange constraint: {const_start} to {const_end}")
        except KeyError:
            const_start = None
            const_end = None

        # Priority 1: user input
        if user_start and user_end:
            # show warnings if appropriate
            if  const_start and user_start < const_start:
                self.warning(f"User time_start {user_start} is before dataset constraint start={const_start}")
            elif const_end and user_end > const_end:
                self.warning(f"User time_end {user_start} is before dataset constraint start={const_end}")
            self.debug(f"user-defined time coverage {user_start} to {user_end}")

            return user_start, user_end

        elif const_start and const_end:
            self.debug(f"constraint-defined time coverage {const_start} to {const_end}")
            return const_start, const_end

        # Lowest priority, get time start and time end directly from the database
        init = time.time()
        db_start, db_end = self.resolve_time_coverage_db(conf, resource)
        self.debug(f"database-defined time coverage {db_start} to {db_end} (took {time.time() - init:.03f} secs)")
        return db_start, db_end

    def add_local_dataset(self, conf: dict, time_start: pd.Timestamp, time_end: pd.Timestamp):
        if not time_start or not time_end:
            self.error("Local datasets MUST have a time range!", exception=True)

        data_type = find_first(conf, target="dataType")
        if not data_type:
            self.error("Could not find data type!", exception=True)

        resource = {
            "id": "local_files",
            "period": "none",
            "format": "csv",
            "host": "localhost",
            "path": "./",
            "dataType": "files"
        }
        rich.print(conf["export"])
        conf["export"]["local"] = {"resources": [resource]}
        self.info("Creating local dataset")
        return conf



    def generate_dataset(self, dataset: str | dict, service_name: str, time_start: pd.Timestamp|str = "",
                         time_end: pd.Timestamp|str = "", fmt: str = "", overwrite=False, erddap_config=False,
                         secrets: dict=None, resources: dict = None, local=False, publish=False,
                         limit:int = 0, no_files=False, update_metadata=False) -> List[DatasetObject,]:
        """

        :param dataset: dataset_id or dataset configuration dict
        :param service_name: name of the service to export
        :param time_start: (optional) time start
        :param time_end: (optional) time end
        :param fmt: override the default format
        :param overwrite: If dataset already exsists, overwrite it or not (True/False)
        :param erddap_config: If erddap_config is True, try to configure remotely the ERDDAP server using emso_metadta_harmonmizer.erddap_config
        :param secrets: secrets dict (needed if erddap_config is True)
        :param resources: generate just a subset of the resources, use a list of resource_id
        :param local: Do not deliver the dataset to the host machine, keep in locally
        :return: list of DatasetObjects
        """
        assert_type(service_name, str)
        assert_types(dataset, [dict, str])
        assert service_name in valid_dataset_services + ["local"], f"Service '{service_name}' not recognized!"

        # Assert that time_start and time_end are correct and with time zone
        time_start = ensure_timestamp(time_start)
        time_end = ensure_timestamp(time_end)

        if type(dataset) is str:
            conf = self.mc.get_document("datasets", dataset)
        else:
            conf = dataset
        dataset_id = conf["#id"]

        self.info(f"=====> Creating dataset {GRN}{dataset_id} {RST} <=====")
        if service_name == "local":  # Force local dataset!
            conf = self.add_local_dataset(conf, time_start, time_end)
        else:
            assert service_name in conf["export"].keys(), f"Dataset {dataset_id} doesn't have export service '{service_name}'"


        # Process special services that do not generate data, but simply link them like Zenodo or CKAN
        if service_name == "ckan":
            if not self.ckan:
                self.error("CKAN not initialized!", exception=ValueError)
            return self.ckan.process_mmapi_dataset(conf, resources=resources)
        elif service_name == "zenodo":
            if not self.zenodo:
                self.error("Zenodo not initialized!", exception=ValueError)
            return self.zenodo.process_mmapi_dataset(conf, resources=resources, publish=publish, tstart=time_start,
                                                     tend=time_end, no_files=no_files, overwrite=overwrite,
                                                     update_metadata=update_metadata)
        elif service_name == "gbif":
            if not self.gbif:
                self.error("GBIF not initialized!", exception=ValueError)
            elif resources:
                raise ValueError("GBIF publication does not accept resources")
            return self.gbif.process_mmapi_dataset(conf, tstart=time_start, tend=time_end, overwrite=overwrite)

        datasets = []


        if not resources:  # By default keep all resources
            resources_obj = conf["export"][service_name]["resources"]
        else: # Keep just the selected resources
            resources_obj =  [r for r in conf["export"][service_name]["resources"] if r["id"] in resources]

        for resource in resources_obj:
            time_start, time_end = self.resolve_time_coverage(conf, resource, time_start, time_end)
            datasets += self.generate_dataset_tree(conf, service_name, resource, time_start, time_end, limit=limit, fmt=fmt,overwrite=overwrite, local=local)

        if service_name == "erddap" and erddap_config:
            try:
                dataset_xml_path = secrets["erddap"]["datasets_xml"]
                dataset_xml_path = secrets["erddap"]["datasets_xml"]
            except KeyError:
                self.error("Could not access datasets.xml path in secrets!", exception=ValueError)

            self.info("Trying to autoconfigure ERDDAP dataset (using last dataset)")

            dataset = None
            for d in datasets:
                if d:
                    dataset = d
            if not datasets:
                self.error("All dataset are empty! Cannot configure ERDDAP", exception=ValueError)

            dataset.configure_erddap_remotely(
                dataset_xml_path,
                big_parent_directory=secrets["erddap"]["big_parent_directory"],
                erddap_uid=secrets["erddap"]["uid"]
            )
        return datasets

    def generate_dataset_tree(self,  dataset: dict, service_name: str, resource: dict, time_start: pd.Timestamp,
                              time_end: pd.Timestamp, fmt: str="", overwrite=False, limit:int=0, local=False):
        assert_type(service_name, str)
        assert_types(dataset, [dict, str])
        assert_types(time_start, [pd.Timestamp, type(None)])
        assert_types(time_end, [pd.Timestamp, type(None)])
        conf = dataset

        if service_name not in conf["export"].keys():
            raise ValueError(f"Dataset {conf['#id']} doesn't have export configuration for service '{service_name}'")

        self.info(f"Generating datasets from {time_start} to {time_end}")

        # Get the period
        intervals = calculate_time_intervals(time_start, time_end, resource["period"])

        datasets = []
        for tstart, tend in intervals:
            d = self.generate_dataset_file(conf, service_name, resource, tstart, tend, fmt=fmt, overwrite=overwrite, limit=limit, local=local)
            if d and not local:
                datasets.append(d)
                d.deliver_and_register()

        return datasets

    def generate_dataset_file(self, conf: dict, service_name: str, resource: dict, time_start: pd.Timestamp,
                              time_end: pd.Timestamp, fmt: str = "", overwrite=False, limit: int=0, local=False) -> DatasetObject|None:
        """
        Generates a dataset based on its configuration stored in Metadata DB
        :param conf: #id of the dataset
        :param service_name: Name of the service that will be used to export the dataset
        :param time_start: dataset time start
        :param time_end: dataset time end
        :param fmt: overwrite original format (e.g. csv instead of netcdf)
        :return: Dataset file
        """
        assert_type(conf, dict)
        assert_type(service_name, str)
        assert_type(resource, dict)
        assert_type(time_start, pd.Timestamp)
        assert_type(time_end, pd.Timestamp)

        self.debug(f"Generating dataset file from {time_start} to {time_end}")
        self.debug(f"Exporting to {service_name}")

        # Convert service ID to dict
        if service_name not in conf["export"].keys():
            raise ValueError(f"Dataset {conf['#id']} doesn't have export configuration for service '{service_name}'")

        if not fmt:
            fmt = resource["format"]
        else:
            assert fmt in dataset_exporter_formats, f"Format '{fmt}' not allowed"

        # Check if the data already exists
        dataset_label = f"{conf["#id"]}:{resource['id']}:{service_name}:{fmt}:{time_start.strftime('%Y-%m-%d')}:{time_end.strftime('%Y-%m-%d')}"
        if self.mc.dataset_resource_exists(conf["#id"], resource['id'], service_name, fmt, time_start, time_end):
            if overwrite:
                # Just throw a warning and continue
                self.info(f"Overwriting existing resource: {dataset_label}")
            else:
                self.error(f"Data resource already exists '{dataset_label}', use the --overwrite flag to overwrite it")
                return None
        if fmt == "csv":
            filename, delivered = self.csv_from_sta(conf, resource, time_start, time_end)
        elif fmt == "netcdf":
            filename, delivered = self.netcdf_from_sta(conf, resource, time_start, time_end, limit=limit)
        elif fmt == "zip":
            filename, delivered = self.zip_from_filesystem(conf, resource, time_start, time_end, limit=limit, overwrite=overwrite)
        elif fmt == "dwca":
            filename, delivered = self.darwin_core_from_sta(conf, resource, time_start, time_end, overwrite=overwrite)
        else:
            raise ValueError(f"Unknown dataSource format '{fmt}'")

        if not filename:
            self.warning("No dataset created!")
            return None
        obj = DatasetObject(self.mc, self.fileserver, conf, filename, service_name, resource, time_start, time_end, fmt,
                            self.boundaries, delivered=delivered)
        self.debug(obj)
        return obj

    def dataframe_from_sta(self, conf: dict, station_ids: list, sensor_ids: list, resource: dict, time_start: pd.Timestamp,
                           time_end: pd.Timestamp, limit=0) -> pd.DataFrame:
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]
        data_type = resource["dataType"]
        if data_type == "timeseries":
            df = self.dataframe_from_sta_timeseries(conf, resource, station_ids, sensor_ids, time_start, time_end, limit=limit)
        elif data_type == "detections":
            df = self.dataframe_from_sta_detections(conf, resource, station_ids, sensor_ids, time_start, time_end)
        elif data_type == "profiles":
            df = self.dataframe_from_sta_profiles(conf, resource, station_ids, sensor_ids, time_start, time_end)
        elif data_type == "files":
            df = self.dataframe_from_sta_files(conf,resource,  station_ids, sensor_ids, time_start, time_end)
        elif data_type == "json":
            df = self.dataframe_from_sta_json(conf, resource,  station_ids, sensor_ids, time_start, time_end)
        else:
            df = None
            self.error(f"Unimplemented data type {conf['dataType']}", exception=ValueError)

        variable_filter = conf.get("@variables", [])
        if variable_filter:
            self.info(f"Filtering variables, keeping: {conf['@variables']}")
            keep_vars = ["time", "depth", "latitude", "longitude", "sensor_id", "platform_id", "field_of_view"] + conf["@variables"]
            for col in df.columns:
                if col.endswith("_QC"):
                    continue
                if col not in keep_vars:
                    self.debug(f"Deleting {col}")
                    del df[col]
                    if col + "_QC" in df.columns:
                        del df[col + "_QC"]

        return df.sort_index(ascending=True)

    def dataframe_from_sta_detections(self, conf: dict, resource: dict,station_ids: list, sensor_ids, time_start: pd.Timestamp,
                                      time_end: pd.Timestamp):
        """
        Return all the detections from a sensor
        """
        raise ValueError("Unimplemented data type detections")

    def add_station_coordinates(self,  df):
        # TODO: Now we keep only the last position. Go through the entire lifetime to get the proper values
        stations = df["platform_id"].unique()
        for coord in ["latitude", "longitude", "depth"]:
            if coord not in df.columns:
                df[coord] = np.nan

        for station in stations:
            latitude, longitude, depth = self.mc.get_station_coordinates(station)
            df.loc[df["platform_id"] == station, "latitude"] = latitude
            df.loc[df["platform_id"] == station, "longitude"] = longitude

            # Add depth only if it didn't exist
            if len(df["depth"].unique()) == 1 and df["depth"].unique()[0] in [None, np.nan]:
                df.loc[df["platform_id"] == station, "depth"] = depth

        return df

    def dataframe_from_sta_generic(self, station_ids: list, sensor_ids, data_type: str, average="", fois=None,
                                   tstart=None, tend=None, first=False, last=False, model_name=None,
                                   limit:int=0):
        """
        This function returns generic DataFrame with the same columns for all data types. The generic dataframe has the
        following columns:
                            timestamp depth     value  qc_flag time_end parameters variable sensor_id platform_id   foi
            2023-01-01 00:00:00+00:00  None  1.000000        1     None       None     CNDC     SBE37       OBSEA  None
            2023-01-01 00:01:40+00:00  None  1.000000        1     None       None     CNDC     SBE37       OBSEA  None
            2023-01-01 00:03:20+00:00  None  1.000000        1     None       None     CNDC     SBE37       OBSEA  None

        This dataset needs to be filtered and pivoted inside the data-specific method.

        :param station_ids:
        :param sensor_ids:
        :param data_type:
        :param average:
        :param fois:
        :return: dataframe with columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        """

        if not fois:
            fois = []

        dataframes = []
        # assert all incoming data types
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        assert_type(sensor_ids, list)
        assert data_type in mmapi_data_types, f"data type '{data_type}' not valid'"
        assert_types(fois, [list, type(None)])
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]
        [assert_type(s, str) for s in fois]
        assert_types(tstart, [type(None), pd.Timestamp])
        assert_types(tend, [type(None), pd.Timestamp])

        assert data_type in mmapi_data_types, f"data type '{data_type}' not valid'"

        # Check incompatible arguments
        if average and data_type in ["files", "json", "detections"]:
            self.error(f"Average on data type {data_type} unimplemented", exception=ValueError)

        self.debug("==== dataframe_from_sta_generic ====")
        self.debug(f"    sensors={sensor_ids}, stations={station_ids}")
        self.debug(f"    dataType={data_type}, average={average}, fois={fois}")
        self.debug(f"    start={tstart}, end={tend}")

        # Step 1: Get the list of datastreams to query
        query = f"""
            select
                "ID"                
            from "DATASTREAMS"
            where
                "SENSOR_ID" in (select "ID" from "SENSORS" WHERE "NAME" in {sql_list(sensor_ids)})
                and "THING_ID" in (select "ID" from "THINGS" WHERE "NAME" in {sql_list(station_ids)})
                and "PROPERTIES"->>'dataType' = '{data_type}'
        """



        # Averaged data needs to be filtered by averagePeriod
        if average:
            query += f""" and "PROPERTIES"->>'averagePeriod' = '{average}'"""
        # In timeseries/profiles/detections we need to be sure to select only fullData
        elif data_type in ["timeseries", "profiles", "detections"]:
            query += f"""     and ("PROPERTIES"->>'fullData')::boolean = True """

        # if there's constraints/@process match we should filter in STA modelName=process
        if model_name:
            query += f"""     and "PROPERTIES"->>'modelName' = '{model_name}' """

        query += ";"
        datastream_ids = self.sta.list_from_query(query)

        # Step 2: Query for data, all queries should return ALL possible column types, regardless if they are empty
        # timestamp, time_end, depth, value, qc_flag, parameters, variable, sensor_id, platform_id, foi
        iso_format = f"%Y-%m-%dT%H:%M:%SZ"

        # If averaged or files/json we need to query OBSERVATIONS table
        if data_type in ["files", "json"] or average:
            self.debug("querying the OBSERVATIONS table")

            # First, select the result column depending on data type
            if data_type in ["timeseries", "profiles", "detections"]:
                result_column = "RESULT_NUMBER"
            elif data_type == "files":
                result_column = "RESULT_STRING"
            else: # json data type
                result_column = "RESULT_JSON"

            query = f"""
                select
                    "OBSERVATIONS"."PHENOMENON_TIME_START" as timestamp,                    
                    "OBSERVATIONS"."PHENOMENON_TIME_END" as time_end,
                    "OBSERVATIONS"."RESULT_TIME" as result_time,
                    "PARAMETERS"->'depth' as depth,
                    "OBSERVATIONS"."{result_column}" as value,
                    "OBSERVATIONS"."RESULT_QUALITY"->>'qc_flag' as qc_flag,
                    "OBSERVATIONS"."PHENOMENON_TIME_END" as time_end,
                    "PARAMETERS" as parameters,
                    "OBS_PROPERTIES"."NAME" AS variable,
                    "SENSORS"."NAME" AS sensor_id,
                    "THINGS"."NAME" AS platform_id,
                    "FEATURES"."NAME" as foi
                
                from "OBSERVATIONS", "SENSORS", "THINGS", "DATASTREAMS", "OBS_PROPERTIES", "FEATURES"
                where
                    "OBSERVATIONS"."DATASTREAM_ID" in {sql_list(datastream_ids, string=False)}
                    --time-start-filter
                    --time-end-filter
                    --foi-filter
                    and "OBSERVATIONS"."DATASTREAM_ID" = "DATASTREAMS"."ID"
                    and "DATASTREAMS"."SENSOR_ID"  =  "SENSORS"."ID"
                    and "DATASTREAMS"."THING_ID" = "THINGS"."ID"
                    and "OBS_PROPERTIES"."ID" = "DATASTREAMS"."OBS_PROPERTY_ID"
                    and "FEATURES"."ID" = "OBSERVATIONS"."FEATURE_ID"
                order by timestamp
                ;"""

            if tstart:
                query = query.replace("--time-start-filter", f"""    and "OBSERVATIONS"."PHENOMENON_TIME_START" >= '{tstart.strftime(iso_format)}'""")
            if tend:
                query = query.replace("--time-end-filter", f"""    and "OBSERVATIONS"."PHENOMENON_TIME_END" < '{tend.strftime(iso_format)}'""")
            if fois:
                foi_ids = self.sta.list_from_query(f"""select "ID" from "FEATURES" where "NAME" in {sql_list(fois)};""")
                query = query.replace("--foi-filter",
                              f"""    and "OBSERVATIONS"."FEATURE_ID" in {sql_list(foi_ids, string=False)}""")
            if first:
                query += query.replace(";", """ order by "OBSERVATIONS"."PHENOMENON_TIME_START" limit 1;""")
            elif last:
                query += query.replace(";", """ order by "OBSERVATIONS"."PHENOMENON_TIME_START" desc limit 1;""")

        else: # Now, deal with timeseries, profiles and detections in the same query
            table_name = data_type  # Data type is the same as table name
            # First, let's decide the columns to query
            if data_type == "timeseries":
                columns = f"timeseries.timestamp, null as depth, timeseries.value, timeseries.qc_flag,"""
            elif data_type == "profiles":
                columns = f"profiles.timestamp,  profiles.depth, profiles.value, profiles.qc_flag,"""
            else: # detections
                columns = f"detections.timestamp, null as depth, detections.value, null as profiles.qc_flag,"""

            query = f"""
                select
                    {columns}
                    null as time_end,
                    null as parameters,                    
                    "OBS_PROPERTIES"."NAME" AS variable,
                    "SENSORS"."NAME" AS sensor_id,
                    "THINGS"."NAME" AS platform_id,
                    null as foi
                from {table_name}, "SENSORS", "THINGS", "OBS_PROPERTIES", "DATASTREAMS"
                where
                    datastream_id in {sql_list(datastream_ids, string=False)}
                    --time-start-filter
                    --time-end-filter                    
                    and {table_name}.datastream_id = "DATASTREAMS"."ID"
                    and "DATASTREAMS"."SENSOR_ID"  =  "SENSORS"."ID"
                    and "DATASTREAMS"."THING_ID" = "THINGS"."ID"
                    and "OBS_PROPERTIES"."ID" = "DATASTREAMS"."OBS_PROPERTY_ID"
                order by timestamp
                ;"""

            if tstart:
                query = query.replace("--time-start-filter", f"""and timestamp >= '{tstart.strftime(iso_format)}'""")
            if tend:
                query = query.replace("--time-end-filter", f"""and timestamp < '{tend.strftime(iso_format)}'""")

            if first:
                query += query.replace(";", f""" order by {table_name}.timestamp limit 1;""")
            elif last:
                query += query.replace(";", f""" order by {table_name}.timestamp desc limit 1;""")

        if limit:
            self.warning(f"Limiting the query to {limit} rows!")
            query = query.replace(";", f"limit {limit};")

        df = self.sta.dataframe_from_query(query)

        # sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        self.add_station_coordinates(df)
        self.debug(df)
        if not df.empty:
            self.boundaries = DatasetBoundaries(df)
        else:
            self.boundaries = None
        return df

    def dataframe_from_sta_timeseries(self, conf: dict, resource: dict, station_ids: list, sensor_ids: list, time_start: pd.Timestamp = None,
                                      time_end: pd.Timestamp = None, limit=0):
        """
        Returns a DataFrame for a specific Sensor in a specific time interval
        """
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]
        data_type = resource["dataType"]
        try:
            avg_period = resource["averagePeriod"]
        except KeyError:
            avg_period=""

        df = self.dataframe_from_sta_generic(station_ids, sensor_ids, data_type, average=avg_period, tstart=time_start, tend=time_end, limit=limit)
        # DataFrame columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        df = df[["timestamp", "depth", "latitude", "longitude", "value", "qc_flag", "variable", "sensor_id", "platform_id"]]
        df = pivot_dataframe(df, pivot_cols=["value", "qc_flag"])
        return df.set_index("timestamp")

    def dataframe_from_sta_profiles(self, conf: dict, resource: dict, station_ids: list, sensor_ids: list, time_start: pd.Timestamp = None,
                                      time_end: pd.Timestamp = None):
        """
        Returns a DataFrame for a specific Sensor in a specific time interval
        """
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]
        data_type = resource["dataType"]
        try:
            avg_period = resource["averagePeriod"]
        except KeyError:
            avg_period=""

        df = self.dataframe_from_sta_generic(station_ids, sensor_ids, data_type, average=avg_period, tstart=time_start, tend=time_end)
        # DataFrame columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        df = df[["timestamp", "depth", "latitude", "longitude", "value", "qc_flag", "variable", "sensor_id", "platform_id"]]
        df = pivot_dataframe(df, pivot_cols=["value", "qc_flag"])
        return df.set_index("timestamp")

    def dataframe_from_sta_files(self, conf: dict, resource: dict, station_ids: list, sensor_ids: list,  time_start: pd.Timestamp = None,
                                      time_end: pd.Timestamp = None):
        """
        Returns a DataFrame for a specific Sensor in a specific time interval
        """
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]
        data_type = resource["dataType"]

        keep_foi = False
        try:
            keep_foi = conf["dataSourceOptions"]["keepFieldOfView"]
        except KeyError:
            self.debug(f"keepFieldOfView not in options, default={keep_foi}")
            pass

        df = self.dataframe_from_sta_generic(station_ids, sensor_ids, data_type, tstart=time_start, tend=time_end)
        # DataFrame columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        df = df[["timestamp", "depth", "latitude", "longitude", "sensor_id", "platform_id", "value", "variable", "foi"]]
        df = pivot_dataframe(df, pivot_cols=["value"])

        if keep_foi:
            df = df.rename(columns={"foi": "field_of_view"})
        else:
            del df["foi"]
        return df.set_index("timestamp")


    def dataframe_from_sta_json(self, conf: dict, resource: dict, station_ids: list, sensor_ids: list, time_start: pd.Timestamp = None, time_end: pd.Timestamp = None):
        """
        Return all the detections from a JSON data
        return: DataFrame with columns: "timestamp", "depth", "value", "parameters", "variable", "sensor_id", "platform_id", "foi"
        """
        assert_type(station_ids, list)
        assert_type(sensor_ids, list)
        [assert_type(s, str) for s in station_ids]
        [assert_type(s, str) for s in sensor_ids]

        data_type = resource["dataType"]
        try:
            avg_period = resource["averagePeriod"]
        except KeyError:
            avg_period=""

        model_name = ""
        if "@processes" in conf["constraints"].keys():
            model_name = conf["constraints"]["@processes"]
        if model_name:
            self.info("Keeping only data from model: " + model_name)
        df = self.dataframe_from_sta_generic(station_ids, sensor_ids, data_type, average=avg_period, tstart=time_start, tend=time_end, model_name=model_name)
        # DataFrame columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        df = df[["timestamp", "depth", "latitude", "longitude", "value", "parameters", "variable", "sensor_id", "platform_id", "foi"]]
        return df.set_index("timestamp")

    def netcdf_from_sta(self, conf: dict, resource: dict, time_start: pd.Timestamp, time_end: pd.Timestamp, limit=0):
        """
        Creates a NetCDF file according to the configuration
        :param conf:
        :param time_start: time start to filter the data
        :param time_end: time start
        :return: generated NetCDF filename
        """
        msg = f"Creating NetCDF dataset for {conf['#id']}, resource={resource['id']}"
        if time_start:
            msg += f" start={time_start}"

        if time_end:
            msg += f" end={time_end}"

        self.info(msg)
        metadata = self.metadata_harmonizer_conf(conf)
        df = self.dataframe_from_sta(conf, conf["@stations"], conf["@sensors"], resource, time_start=time_start, time_end=time_end, limit=limit)
        df = df_netcdf_normalization(df)  # Ensure we have correct strings
        if df.empty:
            self.warning(f"ALL dataframes from {time_start} to {time_end} are empty!, skipping")
            return "", False

        self.info("Generating filename...")
        filename = self.dataset_filename(conf, "netcdf", time_start, time_end)
        self.info("Calling NetCDF wrapper...")
        filename = self.call_dataset_generator(conf, [df], metadata, output=filename)
        self.debug(f"\n{df}")
        self.info(f"Dataset {filename} generated!")
        return filename, False

    def darwin_core_from_sta(self, conf, resource, time_start: pd.Timestamp, time_end: pd.Timestamp, overwrite=False):
        """
        Creates a Darwin Core Archive from JSON data from an AI model
        :param conf:
        :param time_start: time start to filter the data
        :param time_end: time start
        :return: generated NetCDF filenameOBSEA_seabed_station_BGC_L1b
        """
        self.info("Creating Darwin Core Archive dataset")
        assert resource["dataType"] == "json", f"Darwin Core only works with JSON data, got '{resource['dataType']}'"
        assert_type(conf, dict)
        assert_type(resource, dict)
        assert_type(time_start,  pd.Timestamp)
        assert_type(time_end,  pd.Timestamp)
        df = self.dataframe_from_sta(conf, conf["@stations"], conf["@sensors"], resource, time_start=time_start, time_end=time_end)
        self.log.debug(f"Creating dataset from data: \n{df}")
        if df.empty:
            self.warning(f"Dataset {conf['#id']}:{resource['id']} has no data from {time_start} to {time_end}")
            return "", False

        dwca = DarwinCoreArchive(self.mc, self.sta, df, conf, time_start, time_end, self.log)
        filename = self.dataset_filename(conf, "dwca", time_start, time_end)
        dwca.create_archive(filename)
        return filename, False


    def csv_from_sta(self, conf, resource, time_start: pd.Timestamp, time_end: pd.Timestamp):
        """
        Generates a CSV file from a SensorThings Database
        """
        filename = self.dataset_filename(conf, "csv", time_start, time_end)
        df = self.dataframe_from_sta(conf, conf["@stations"], conf["@sensors"], resource, time_start, time_end)
        if df.index.name == "timestamp":
            df.index.name = "time"

        df.to_csv(filename)
        self.info(f"Writing CSV file '{filename}'")
        return filename, False

    def zip_from_filesystem(self, conf, resource, time_start, time_end, overwrite=False, limit: int = 0) -> (str, bool):
        """
        Compresses all files in the fileserver into a zip file. Since millions of files can be compressed, a small
        bash script will be generated and transferred to the fileserver and executed there. Then the file will be
        transferred to the machine running MMAPI

        :return filename, delivered
        """
        dataset_id = conf["#id"]

        # Create the dataset in /opt/temp
        self.info(f"Creating ZIP dataset, ID: {conf['#id']}, from {time_start} to {time_end}")


        try:
            # Check if there are fieldOfView constrains in the dataset
            fois = [conf["constraints"]["fieldOfView"]["@programmes"]]
        except KeyError:
            fois = []

        # Sometimes, PNG files need to be compressed to JPEG to reduce dataset size
        jpeg_compression = False
        if "dataSourceOptions" in conf.keys() and "jpegCompression" in conf["dataSourceOptions"].keys():
            jpeg_compression = conf["dataSourceOptions"]["jpegCompression"]

        # Getting dataframe
        df = self.dataframe_from_sta_generic(conf["@stations"], conf["@sensors"], "files", tstart=time_start,
                                             tend=time_end, fois=fois, limit=limit)
        # DataFrame columns: timestamp, depth, value, qc_flag, time_end, parameters, variable, sensor_id, platform_id, foi
        df = df[["timestamp", "value", "sensor_id", "platform_id", "foi"]]
        df = df.rename(columns={"value": "URL"})
        if df.empty:
            self.error(f"could not generate dataset {conf['#id']}:{resource['id']} from {time_start} to {time_end}")
            return "", False

        remote_filename = self.dataset_filename(conf, "zip", time_start, time_end, tmp_folder="/opt/temp")

        tmp_folder = f"/opt/temp/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

        # Now we will do the following:
        # 1. Create a temporal folder in /var/temp/<epochtime>
        # 2. Create one folder per sensor:
        #     /var/temp/<epochtime>/<sensor1>
        #     /var/temp/<epochtime>/<sensor2>
        # 3. To avoid errors due to long comands over ssh, we will put all the required stuff inside a bash script
        #    and send it to the server the script will have:
        #    3.1 copy all files in the DataFrame to its sensor folder
        #    3.2 create index.csv file
        #    3.3 compress to zip
        #    3.3 send the zip file to its destination
        #    3.4 delete temporal files

        files = list(df["URL"])  # List of all files to be compressed

        if len(files) < 1:
            raise ValueError(f"No files to be zipped!")
        elif len(files) == 1:
            self.warning(f"Just one file!")
            raise ValueError(f"Just one file to be zipped?")

        # Now convert the file URLs to filesystem paths
        files = [self.fileserver.url2path(f) for f in files]
        df["src_files"] = files
        dst_files = []
        folders = []
        for _, row in df.iterrows():
            pic_folder = row["timestamp"].strftime("%Y/%m/%d")
            if pic_folder not in folders:
                folders.append(pic_folder)
            dst_files.append(pic_folder + "/" + os.path.basename(row["src_files"]))

        df["files"] = dst_files

        # If fileserver is localhost and file paths are relative, convert them to absolute
        if self.fileserver.host == "localhost" or self.fileserver.host == socket.gethostname():
            if not df["src_files"].values[0].startswith("/"):
                self.info("Converting relative file paths to absolute ones before zipping files")
                df["src_files"] = [os.path.abspath(f) for f in df["src_files"]]

        dfcsv = df.copy()
        dfcsv = dfcsv

        del dfcsv["src_files"]
        dfcsv.to_csv("index.csv", index=False)
        # If the command is too long it cannot be sent via ssh and will raise an OSError, we create a temporal script
        # with the command and send it to the host
        script_name = os.path.basename(remote_filename).split(".")[0] + ".sh"
        self.info(f"Creating zip script {script_name}...")

        # create sensor folders
        abs_folders = [os.path.join(tmp_folder, f) for f in folders]
        run_over_ssh(self.fileserver.host, f"mkdir -p {' '.join(abs_folders)}")

        # Send index.csv file
        self.fileserver.send_file(tmp_folder, "index.csv", indexed=False)
        os.remove("index.csv")  # remove local index.csv

        # ======== Creating Bash script to create zip remotely ========#
        cmd = "#!/bin/bash\n"
        cmd += "set -o errexit\n"
        cmd += "set -o nounset\n"
        cmd += "echo 'Auto-generated script from MMAPI, compressing files into a zip file'\n"
        cmd += f"cd {tmp_folder}\n"
        for _, row in df.iterrows():
            source = row["src_files"]
            dest = row["files"]
            if jpeg_compression and source.endswith(".png"):
                # do not copy, instead convert to JPEG
                cmd += f"convert -quality 95 {source} {dest.replace('.png', '.jpg')}\n"
            else:
                # directly copy
                cmd += f"ln -s {source} {dest}\n"

            # At this point the file should be created
            # if the destination and the fileserver are the same (very likely), just copy from temp folder to the definitive
            # folder

        if self.fileserver.host != resource["host"]:
            self.error("Not implemented! FileServer and destination are not the same?", exception=ValueError)

        dest = resource["path"]
        src = remote_filename
        self.info(f"Moving inside fileserver from {src} to {dest}")
        remote_filename = os.path.join(dest, os.path.basename(src))
        cmd += f"zip -6 -rn .jpg:.jpeg:.mp4:.png:.nc -r {remote_filename} index.csv {' '.join(folders)}\n"
        cmd += f"rm -rf {tmp_folder}\n"

        with open(script_name, "w") as f:
            f.write(cmd)  # write the command to the script
        os.chmod(script_name, 0o775)

        self.debug(f"Delivering script...")
        script_dest = os.path.join(f"{tmp_folder}")
        self.fileserver.send_file(script_dest, script_name, indexed=False)
        if jpeg_compression:
            self.info(f"JPEG compression enabled, this will take even longer than usual!")
        self.info(f"Running script to create zip file with {len(files)} files, this may take a while...")
        # Run the script!
        t = time.time()
        run_over_ssh(self.fileserver.host, script_dest + "/" + script_name, fail_exit=True)
        self.info(f"Script executed, it took {time.time() -t :.02f} seconds")

        delivered = True

        # Check if the size is coherent
        a = run_over_ssh(self.fileserver.host, f"ls -l {remote_filename}")
        size = int(a.split(" ")[4])  # size is column 5 of ls -l command
        if size < 3000:
            self.warning(f"first url: {df['urls'].values[0]}")
            self.warning(f"last  url: {df['urls'].values[-1]}")
            self.warning(f"Remote filename: {remote_filename}")
            self.error(f"ZIP file looks empty (file size {human_readable_bytes(size)})! less than 3k means there's nothing inside", exception=ValueError)

        os.remove(script_name)
        return remote_filename, delivered


    def metadata_harmonizer_conf(self, dataset: dict, tstart: pd.Timestamp = None, tend: pd.Timestamp = None,
                                 default_data_mode="delayed") -> dict:
        """Generates the EMSO Metadata Harmonizer metadata document based in metadata DB

        """
        if tstart:
            assert_type(tstart, pd.Timestamp)
        if tend:
            assert_type(tend, pd.Timestamp)

        sensors = [self.mc.get_document("sensors", sensor_id) for sensor_id in dataset["@sensors"]]

        var_subset = []
        if "@variables" in dataset.keys() and dataset["@variables"] is not None:
            var_subset = dataset["@variables"]

        # Let's create a dict where key=varname, value=units
        variable_units = {}
        for sensor in sensors:
            for var in sensor["variables"]:
                varname = var["@variables"]
                units = var["@units"]
                if var_subset and not varname in var_subset:
                    self.debug(f"Excluding variable '{varname}', subset={var_subset}")
                elif varname not in variable_units.keys():
                    self.debug(f"Adding {varname}")
                    variable_units[varname] = units
                else: # variable already added, so make sure that units are the same
                    registered = variable_units[varname]
                    assert units == registered, f"Units for '{varname}' not consistent! '{units}' != '{registered}'"

        # Using first station to get owner
        station = self.mc.get_document("stations", dataset["@stations"][0])

        # Get minimum info (PI and owner)
        pi, _ = self.mc.get_contact_by_role(dataset, "ProjectLeader")
        owner, _ = self.mc.get_contact_by_role(station, "owner")
        institution_str = owner["acronym"] + " - " + owner["fullName"]

        # Put all people involved in an array
        people = []
        roles = []
        for c in dataset["contacts"]:
            role = c["role"]
            if "@organizations" in c.keys():
                continue
            name = self.mc.get_document("people", c["@people"])["name"]
            people.append(name)
            roles.append(role)

        # Put funding information
        project_names = []
        project_codes = []
        if "funding" in dataset.keys():
            for project_id in dataset["funding"]["@projects"]:
                project = self.mc.get_document("projects", project_id)
                project_names.append(project["acronym"])
                project_codes.append(project["funding"]["grantId"])

        # Get the OceanSITES Data Mode
        data_mode = default_data_mode
        if "dataMode" in dataset.keys():
            data_mode = dataset["dataMode"]
        data_mode_dict = {"real-time": "R", "delayed": "D", "mixed": "M", "provisional": "P"}
        dm = data_mode_dict[data_mode]

        edmo_code = owner["EDMO"]
        if edmo_code.startswith("http"):
            edmo_code = edmo_code.split("/")[-1]
        meta = {
            "global": {
                "title": dataset["title"],
                "summary": dataset["summary"],
                "Conventions": "OceanSITES EMSO CF-1.8",
                "institution": institution_str,
                "institution_edmo_code": edmo_code,
                "institution_ror_uri": owner["ROR"],
                "update_interval": "void",
                "source": station["platformType"]["label"],
                "data_type": "OceanSITES profile data",
                "format_version": "1.4",
                "network": "EMSO",
                "data_mode": dm,
                "projects": "",
                "project_codes": "",
                "principal_investigator": pi["name"],
                "principal_investigator_email": pi["email"],
                "license": "CC-BY-4.0",
                "contributor_name": people,
                "contributor_role": roles,
            },
            "platforms": {},
            "sensors": {},
            "variables": {}
        }


        optional_args = {
            "oso/regionalFacility/label": "emso_regional_facility_name",
            "oso/site/label": "emso_site_name"
        }
        populate_dict(station, meta["global"], optional_args)

        for sensor in sensors:
            sensor_id = sensor["#id"].replace("-", "_")
            self.debug(f"Assuming sensor '{sensor_id}' is mounted_on_seafloor_structure")

            if sensor["instrumentType"]["label"] == "cameras":
                orientation = "horizontal"
            else:
                orientation = "upward"
            self.debug(f"Assuming sensor '{sensor_id}' is orientation is '{orientation}'")

            meta["sensors"][sensor_id] = {
                "long_name": sensor["longName"],
                "sensor_serial_number": sensor["serialNumber"],
                "sensor_mount": "mounted_on_seafloor_structure",
                "sensor_orientation": orientation,
                "sdn_instrument_uri": sensor["model"]["definition"],
                "sensor_manufacturer_uri": sensor["manufacturer"]["definition"],
                "sensor_type_uri": sensor["instrumentType"]["definition"]
            }

        for variable_id, units_id in variable_units.items():
            variable = self.mc.get_document("variables", variable_id)
            units = self.mc.get_document("units", units_id)
            self.debug(f"   getting {variable_id} with units {units['symbol']}")
            varname = variable_id.replace("-", "_").replace(" ", "_")
            if variable["type"] == "environmental":
                meta["variables"][varname] = {
                    "long_name": variable["description"],
                    "sdn_parameter_uri": variable["definition"],
                    "sdn_uom_uri": units["definition"],
                    "standard_name": variable["standard_name"],
                }
            elif variable["type"] in ["biological", "biodiversity"]:
                raise ValueError(f"Unimplemented! {variable_id}")

            elif variable["type"] == "technical":
                meta["variables"][varname] = {
                    "long_name": variable["description"],
                    "variable_type": "technical",
                    "comment": variable["description"]
                }
            else:
                raise ValueError(f"Type '{variable['type']}' not valid!")


        for station_id in dataset["@stations"]:
            platform = self.mc.get_document("stations", station_id)
            platform_name = station_id.replace("-", "_").replace(" ", "_")
            self.warning(f"Assuming that station '{platform['#id']}' is fixed and has lat,lon,depth")
            latitude, longitude, depth = self.mc.get_station_position(station["#id"], tstart)

            meta["platforms"][platform_name] = {
                "long_name": platform["longName"],
                "platform_type_name": platform["platformType"]["label"],
                "platform_type_uri": platform["platformType"]["definition"],
                "platform_reference": platform["platformType"]["definition"],

                "depth": depth,
                "latitude": latitude,
                "longitude": longitude
            }

            # Fill optional arguments
            optional_args = {
                "wmo_number": "wmo_platform_code",
                "oso/platform/label": "emso_platform_name",
            }
            populate_dict( platform,  meta["platforms"][platform_name], optional_args)

        if  "keepFieldOfView" in dataset["dataSourceOptions"].keys() and dataset["dataSourceOptions"]["keepFieldOfView"]:
            self.debug("Adding field_of_view metadata")
        meta["variables"]["field_of_view"] = {
            "long_name": "Field of View",
            "variable_type": "technical",
            "comment": "Short description of where the camera is pointing at or the objects within the field of view"
        }


        return meta


    def call_dataset_generator(self, conf: dict, dataframes: list, metadata: dict, output="output.nc"):
        """
        Dump dataframes and metadata to temporal files and calls the datasets generator
        :param dataframes:
        :param metadata:
        :param output:
        :return:
        """
        assert_type(dataframes, list)
        [assert_type(df, pd.DataFrame) for df in dataframes]
        assert_type(metadata, dict)
        assert_type(output, str)

        # The metadata expander handles special cases where the variables listed do not math with the data columns,
        # like AI-produced data for object detections.
        metadata_expander = {
            # Key -> variable name, value -> function that processes the metadata with conf, meta data args
            "FATX": self.fatx_metadata_expander,
        }

        if not self.emso:
            self.emso = emso_metadata_harmonizer.metadata.EmsoMetadata()
        # dataframes = [df.reset_index() for df in dataframes]
        data_files = []

        unique_id = uuid.uuid4()

        for i, df in enumerate(dataframes):
            f = f".temp_{i:02d}_{unique_id}.csv"
            df.to_csv(f)
            data_files.append(f)

        meta_file = f"meta_{unique_id}.yaml"
        with open(meta_file, "w") as f:
            yaml.dump(metadata, f)

        def temp_files_cleanup(tmp_files: list):
            for f in tmp_files:
                if os.path.exists(f):
                    os.remove(f)
        try:
            emh.generate_dataset(data_files, [meta_file], output=output)
        except Exception as e:
            temp_files_cleanup(data_files + [meta_file])
            raise e

        temp_files_cleanup(data_files + [meta_file])

        return output


    def fatx_metadata_expander(self, conf: dict, meta: dict, df: pd.DataFrame):
        """
        FATX is used for datasets where fish abundance has been estimated probably with an object detection algorithm.
        It is assumed that it is an underwater_photography dataset.
        """
        assert_type(conf, dict)
        assert_type(meta, dict)
        assert_type(df, pd.DataFrame)
        try:
            ai_model = conf["constraints"]["@processes"]
        except KeyError:
            self.error("Could not find @processes reference in FATX metadata!", exception=ValueError)
        self.info(f"FATX metadata expander with model '{ai_model}'")
        process = self.mc.get_document("processes", ai_model)
        variables = process["variableNames"]

        # rename what needs to be renamed
        if "rename" in process.keys():
            for i, var in enumerate(variables):
                if var in process["rename"].keys():
                    variables[i] = process["rename"][var]

        if "ignore" in process.keys():
            for var in process["ignore"]:
                if var in variables:
                    del variables[variables.index(var)]

        var_list = list(np.unique(variables))

        db_variables = self.mc.get_documents("variables")
        # create a dictionary with standard_name as key and doc as value
        variable_dict = {doc["standard_name"]: doc for doc in db_variables if doc["standard_name"]}

        meta["variables"] = {}
        for var in var_list:
            variable_doc = variable_dict[var]
            meta["variables"][var] = {
                "*long_name": f"abundance of {var} detected by AI model '{ai_model}'",
                "*sdn_parameter_uri": variable_doc["definition"],
                "~sdn_uom_uri": "Dimensionless",
                "~standard_name": var,
            }

        if "SourceImage" in df.columns:
            variable_doc = self.mc.get_document("variables", "underwater_photography")
            meta["variables"][var] = {
                "*long_name": f"underwater images",
                "*sdn_parameter_uri": variable_doc["definition"],
                "~sdn_uom_uri": "Dimensionless",
                "~standard_name": variable_doc["standard_name"]
            }

        if "ProcessedImage" in df.columns:
            variable_doc = self.mc.get_document("variables", "underwater_photography")
            meta["variables"][var] = {
                "*long_name": f"underwater images with AI detections",
                "*sdn_parameter_uri": variable_doc["definition"],
                "~sdn_uom_uri": "Dimensionless",
                "~standard_name": variable_doc["standard_name"]
            }

        return meta

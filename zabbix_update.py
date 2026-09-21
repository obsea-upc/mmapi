#!/usr/bin/env python3
"""
This script automatically registers all data from the MMAPI and SensorThings database into a Zabbix server. All stations
are registered as host groups and real-time sensors are registered as hosts. Real-time sensors are those whose dataMode
prperty is "real-time":

    {   ...
        "dataMode": "real-time"
    }

Latest data from real-time sensors is also sent to zabbix according to period. To avoid duplicated data, times are
floored to prevent duplicated data. So, if period="30min" and the script is executed at 11:04, ALL data from 10:30 to
11:00 will be sent. If the period="1h" and the script is executed at 11:57 all data from 10:00 to 11:00 will be sent.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 29/10/24
"""
from argparse import ArgumentParser
import pandas as pd
import yaml
from mmm import setup_log, SensorThingsApiDB
from mmm.common import LoggerSuperclass, PRL, RST, GRN, assert_type
from mmm.metadata_collector import init_metadata_collector
from zabbix_utils import ZabbixAPI, Sender, ItemValue
import zabbix_utils
import time
import rich
import logging


def gen_zbx_key(sensor_id, varname, data_type, average=""):
    """
    Generates a zabbix item key based on the input info
    :param sensor_id:
    :param varname:
    :param data_type:
    :param average:
    :return: key value. If a key could not been generated, just
    """
    key = f"{sensor_id}.{varname}.{data_type}"
    if data_type in ["timeseries", "profiles"]:
        # only profiles and timeseries can be averaged
        if not average:
            key += ".full"
        else:
            key += f".{average}_avg"
    return key



class ZabbixUpdater(LoggerSuperclass):
    def __init__(self, secrets: dict, log: logging.Logger, sensor_subset=[]):

        assert_type(secrets, dict)
        assert_type(log, logging.Logger)
        assert_type(sensor_subset, list)
        [assert_type(sensor, str) for sensor in sensor_subset]
        LoggerSuperclass.__init__(self, log, name="ZBX", colour=PRL)

        mc = init_metadata_collector(secrets, log=log)

        staconf = secrets["sensorthings"]
        self.mc = mc
        self.sta = SensorThingsApiDB(staconf["host"], staconf["port"], staconf["database"], staconf["user"],
                                         staconf["password"], log, timescaledb=True)

        self.info("Retrieving data from MetadataCollector")

        sensors = mc.get_documents("sensors")

        if sensor_subset:
            sensors = [s for s in sensors if s["#id"] in sensor_subset]

        self.sensors = sensors
        self.active_sensors = []  # list of sensors providing real-time data currently deployed
        stations = mc.get_documents("stations")
        activities = mc.get_documents("activities")

        self.info(f"Got {len(sensors)} sensors")
        self.info(f"Got {len(stations)} stations")
        self.info(f"Got {len(activities)} activities")

        self.info("Connecting to Zabbix")
        self.api = ZabbixAPI(url=secrets["zabbix"]["url"])
        self.api.login(token=secrets["zabbix"]["api_key"])
        logging.getLogger("zabbix_utils").setLevel(logging.WARNING)
        logging.getLogger("concurrent").setLevel(logging.WARNING)

        self.sender = Sender(server=secrets["zabbix"]["host"], port=secrets["zabbix"]["port"])

        self.info("Processing stations...")
        self.register_stations(stations)

        # DataFrame with the columns: datastream_id, varname, sensor_id, full_data, data_type, average_period
        df = self.sta.dataframe_from_query("""
            select 
                "DATASTREAMS"."ID" as datastream_id,
                "OBS_PROPERTIES"."NAME" as varname,
                "SENSORS"."NAME" as sensor_id,            
                ("DATASTREAMS"."PROPERTIES"->'fullData')::boolean as full_data,
                "DATASTREAMS"."PROPERTIES"->>'dataType' as data_type,            
                "DATASTREAMS"."PROPERTIES"->>'averagePeriod' as average_period	
            from "DATASTREAMS", "OBS_PROPERTIES", "SENSORS" where 
                "OBS_PROPERTIES"."ID" = "DATASTREAMS"."OBS_PROPERTY_ID" 
                and "DATASTREAMS"."SENSOR_ID" = "SENSORS"."ID"
            order by  "DATASTREAMS"."ID" asc
            ;"""
        )


        if sensor_subset:
            df = df[df["sensor_id"].isin(sensor_subset)]

        self.debug(f"\n{df}")
        self.datastreams = df
        self.register_sensors(sensors)
        self.update_sensor_status()

    def update_sensor_status(self):
        """
        Updates the enabled/disabled state in Zabbix for hosts in the OBSEA-related host groups
        based on the sensor's last deployment status in MMAPI.
        """
        self.info("Updating sensor status for OBSEA host groups...")
        host_groups = self.get_host_groups()
        hosts = self.get_hosts()

        target_group_names = {"OBSEA", "OBSEA_Besos_Buoy"}
        target_group_ids = {
            host_groups[group_name]["groupid"]
            for group_name in target_group_names
            if group_name in host_groups
        }

        if not target_group_ids:
            self.warning("No target host groups found for sensor status update")
            return

        # Keep one entry per host, even if it belongs to both groups
        target_hosts = {}
        for host in self.api.host.get(groupids=list(target_group_ids)):
            target_hosts[host["name"]] = host

        for sensor_id, host in target_hosts.items():
            try:
                _, deployment_time, active = self.mc.get_last_sensor_deployment(sensor_id)
            except LookupError:
                self.debug(f"Ignored sensor {sensor_id} with no deployment")
                continue

            desired_status = "0" if active else "1"  # Zabbix: 0=enabled, 1=disabled
            current_status = host["status"]

            if current_status == desired_status:
                self.debug(f"Host {sensor_id} already matches desired status active={active}")
                continue

            self.info(f"Updating host {sensor_id} to enabled={active}, deployed on {deployment_time}")
            self.api.host.update({
                "hostid": host["hostid"],
                "status": desired_status,
            })

    def get_hosts(self) -> dict:
        hosts = self.api.host.get()
        return {h["name"]: h for h in hosts}  # convert to dict with name as key

    def get_host_groups(self) -> dict:
        groups = self.api.hostgroup.get()
        return {g["name"]: g for g in groups}  # convert to dict with name as key

    def register_stations(self, stations: list):
        """
        Registers all mmapi stations as zabbix host groups
        :param stations:  list of stations
        """
        self.info("processing stations...")
        host_groups = self.get_host_groups()
        for station in stations:
            if station["#id"] not in list(host_groups.keys()):
                log.info(f"    registering station {station['#id']} as host group")
                self.api.hostgroup.create({"name": station['#id']})

    def register_sensors(self, sensors: list):
        """
        Registers all mmapi stations as zabbix host groups.
        :param stations:  list of stations
        """
        self.info("processing sensors...")
        hosts = self.get_hosts()
        host_groups = self.get_host_groups()

        for sensor in sensors:
            sensor_id = sensor["#id"]

            # Work only with sensor with dataMode=real-time
            realtime = sensor["dataMode"] == "real-time"
            if not realtime:
                self.debug(f"Ignored sensor {sensor_id} with delayed mode")
                continue

            try:
                station_id, timestamp, active = self.mc.get_last_sensor_deployment(sensor_id)
            except LookupError:
                self.warning(f"Ignored sensor {sensor_id} with no deployment")
                continue

            if active:
                self.active_sensors.append(sensor_id)

            if active:  # WARNING: Zabbix inverts logic 0 for active and 1 for inactive
                mmapi_host_status = "0"  # 0 monitored host
            else:
                mmapi_host_status = "1"  # 1 unmonitored host

            # Check if the sensor is registered in zabbix
            if  sensor_id in list(hosts.keys()):
                # If sensor is deployment is active host should be enabled, otherwise disabled
                host = hosts[sensor_id]
                hostid = host["hostid"]
                zbx_host_status = host["status"]
                if zbx_host_status != mmapi_host_status:
                    # Host status (zabbix) and deployment (mmapi) do not match, update zabbix!
                    self.info(f"Updating host {sensor_id} to enabled={active}")
                    h = {"hostid": hostid, "status": mmapi_host_status}
                    self.api.host.update(h)

            else:
                # Register sensor as new Host
                station_host_groupid = host_groups[station_id]["groupid"]
                self.info(f"    registering sensor {sensor['#id']} as host")
                host = {
                    "host": sensor['#id'],
                    "status": str(int(active)),
                    "groups":  [{"groupid": station_host_groupid}],
                    "tags": [
                        {"tag": "instrumentType", "value": sensor["instrumentType"]["label"]}
                    ],
                }
                self.info(f"Registering host={GRN}'{sensor_id}'{RST} with enabled={active}")
                host = self.api.host.create(host)
                hostid =  host["hostids"][0]

                # now register variables
                df = self.datastreams
                df = df[df["sensor_id"] == sensor_id]
                self.register_variables(df, hostid)


    def register_variables(self, df, hostid):
        for _, row in df.iterrows():
            self.register_variable(hostid, row["sensor_id"], row["varname"], row["data_type"], row["average_period"])



    def register_variable(self, hostid: str, sensor_id: str, varname: str, data_type: str, period: str,
                          no_data_time=3600):
        """

        :param hostid: zabbix host id
        :param sensor_id: sensor ID in mmapi
        :param varname: variable name
        :param data_type: data type
        :param period: period of the average, "" if full data
        :param no_data_time: default time in seconds to set the no data alarm for full data datastreams. Only used if
        period is empty

        :return:
        """

        key = gen_zbx_key(sensor_id, varname, data_type, average=period)

        # Zabbix Items data types
        # 0 - numeric float;
        # 1 - character;
        # 2 - log;
        # 3 - numeric unsigned;
        # 4 - text;
        # 5 - binary.
        # More info at https://www.zabbix.com/documentation/current/en/manual/api/reference/item/object


        if data_type in ["json", "files"]:
            value_type = '4'  # text
        elif data_type == "detections":
            value_type = '3'  # integer
        else:
            value_type = '0'  # by default use floats

        self.info(f"    key={key}")
        item = {
            "name": key,
            "key_": key,
            "hostid": hostid,
            "type": '2',  # zabbix trapper
            "value_type": value_type,
            "allow_traps": '1'
        }

        if data_type == "detections":
            # Do not register trigger for detections, data not acquired periodically
            return

        if period:  # Calculate specific no data time for average data
            period_seconds = int(pd.to_timedelta(period).to_numpy()//1e9)
            no_data_time = 2*period_seconds
        try:
            self.api.item.create(item)
        except zabbix_utils.exceptions.APIRequestError:
            self.warning(f"Cannot register '{key}', maybe it is duplicated?")
            return

        # Create NoData trigger
        trigger = {
            "description": f"{key} no data",
            "expression": f"nodata(/{sensor_id}/{key}, {no_data_time})",
            "priority": "3",  # average
            "type": "0"  # do not generate multiple events
        }
        self.api.trigger.create(trigger)


    def send_last_data(self, period: str):

        from_time = (pd.Timestamp.now(tz="UTC") - pd.to_timedelta(period)).floor(period)
        end_time = from_time + pd.to_timedelta(period)

        for _, row in self.datastreams.iterrows():
            sensor_id = row["sensor_id"]
            varname = row["varname"]
            average = row["average_period"]
            data_type = row["data_type"]
            datastream_id = row["datastream_id"]
            key = gen_zbx_key(sensor_id, varname, data_type, average)
            if sensor_id not in self.active_sensors:
                self.debug(f"ignoring disabled variable {key}")
                continue
            if data_type in ["timeseries", "profiles", "detections"] and not average:
                self.send_latest_from_timescaledb(datastream_id, key, sensor_id, data_type, from_time, end_time)
            else:
                self.send_latest_from_observations(datastream_id, key, sensor_id, data_type, from_time, end_time)


    def send_latest_from_timescaledb(self, datastream_id, key, sensor_id, data_type, from_time, end_time):
        """
        Sends latest data from timeseries, profiles or detections hypertables.
        :param datastream_id:
        :param sensor_id:
        :param varname:
        :param data_type:
        :param average:
        :param from_time:
        :param end_time:
        :return:
        """

        df = self.sta.dataframe_from_query(
            f"select timestamp, value from {data_type} where datastream_id = {datastream_id} "
            f"and timestamp between '{from_time}' and '{end_time}';"
        )
        if df.empty:
            self.debug(f"No data for {key}")
            return

        if data_type == "detections":
            # Do not inject detections
            return
        last_t = pd.Timestamp(df["timestamp"].values[-1]).strftime("%Y-%m-%dT%H:%M:%S")
        last_v = df["value"].values[-1]
        self.info(f"{key}: {len(df)} data points (last {last_t}  {last_v})")
        self.send_from_df(df, sensor_id, key)


    def send_latest_from_observations(self, datastream_id, key, sensor_id, data_type, from_time, end_time):

        # First select which column from the OBSERVATIONS table to read
        if data_type == "json":
            column = "RESULT_JSON"
        elif data_type == "files":
            column = "RESULT_STRING"
        else:
            column = "RESULT_NUMBER"

        df = self.sta.dataframe_from_query(
            f'select "PHENOMENON_TIME_START" as timestamp, "{column}" as value from "OBSERVATIONS" '             
            f'where "DATASTREAM_ID" = {datastream_id    } '
            f"and \"PHENOMENON_TIME_START\" between '{from_time}' and '{end_time}';"
        )

        if df.empty:
            self.debug(f"{key}: no data")
            return
        last_t = pd.Timestamp(df["timestamp"].values[-1]).strftime("%Y-%m-%dT%H:%M:%S")
        last_v = df["value"].values[-1]
        self.info(f"{key}: {len(df)} data points (last {last_t}  {last_v})")
        self.send_from_df(df, sensor_id, key)


    def send_from_df(self, df, sensor_id, key):
        items = []
        for timestamp, value in zip(df["timestamp"].values, df["value"].values):
            epoch = timestamp.astype('int64') // 10 ** 9  # Divide by 10^9 to get seconds
            items.append(ItemValue(sensor_id, key, str(value), epoch))
        r = self.sender.send(items)

        if r.total != len(df):
            self.error(f"{key} expected processing {len(df)}, but zabbix only processed {r['total']}")
        if r.total != r.processed:
            self.error(f"{key} got {r.failed} errors when processing {len(df)}")


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("-v", "--verbose", help="verbose output", action="store_true")
    argparser.add_argument("-p", "--period", help="Period to be processed", default="30min", type=str)
    argparser.add_argument("-S", "--sensors", help="Sensor subset", nargs="+", type=str, default=[])

    args = argparser.parse_args()

    # Step 1: Get all data from the Metadata Database. We need all sensors, stations and activities
    t = time.time()
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]

    log_level = "info"
    if args.verbose:
        log_level = "debug"

    log = setup_log("zabbix_updater", log_level=log_level)

    zbx = ZabbixUpdater(secrets, log, sensor_subset=args.sensors)
    zbx.send_last_data(args.period)
    log.info(f"Total elapsed time={time.time() - t:.02f} seconds")
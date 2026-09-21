#!/usr/bin/env python3
"""
This file implements the MetadataCollector, a class implementing metadata access / storage to a PostgresQL database
storing JSON docs.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez@upc.edu
license: MIT
created: 30/11/22
"""
import logging
import time
from typing import Tuple, List

import jsonschema
from numpy.core.defchararray import upper

from mmm.data_sources.postgresql import PgDatabaseConnector, sql_list
import datetime
import json
import pandas as pd
import os
from mmm.common import YEL, RST, load_fields_from_dict, validate_schema, PRL, setup_log, assert_type, assert_types, \
    check_url, strip_metadata_fields
from mmm.common import LoggerSuperclass
import psycopg2
from psycopg2 import sql
import rich

try:
    from mmm.schemas import mmm_schemas, mmm_metadata
except ImportError:
    from schemas import mmm_schemas, mmm_metadata


def get_timestamp_string():
    now = datetime.datetime.now(datetime.UTC).isoformat()
    now = now.split(".")[0] + "Z"
    return now


def validate_key(data, key, key_type, errortype=SyntaxError):
    if key not in data.keys():
        raise errortype(f"Required key {key} not found")
    elif type(data[key]) is not key_type:
        raise errortype(f"Expected type of {key}  is {key_type}, got {type(data[key])}")


def init_metadata_collector(secrets: dict, log=None):
    """
    Initializes a Metadata Collector object from secrets file
    :param secrets: dict object from the secrets yaml file
    :returns: MetadataCollector object
    """
    assert "mmapi" in secrets.keys(), "mmapi key not found!"
    __required_keys = [
        "connection",
        "default_author",
        "organization"
    ]
    for key in __required_keys:
        assert key in secrets["mmapi"].keys(), f"key '{key}' not found in secrets[\"mmapi\"]"

    if not log:
        log = setup_log("MC")

    return MetadataCollector(secrets["mmapi"]["connection"],
                             secrets["mmapi"]["default_author"],
                             secrets["mmapi"]["organization"],
                             log)


def init_metadata_collector_env(log=None):
    """
    Initializes a Metadata Collector object from environment variables
    :returns: MetadataCollector object
    """
    __required_keys = [
        "mmapi_connection",
        "mmapi_default_author",
        "mmapi_organization"
    ]
    for key in __required_keys:
        assert key in os.environ.keys(), f"key '{key}' not environment variables"

    if not log:
        log = setup_log("MC")

    return MetadataCollector(os.environ["mmapi_connection"],
                             os.environ["mmapi_default_author"],
                             os.environ["mmapi_organization"],
                             log)


def postgres_results_to_dict(results, time_format="%Y-%m-%dT%H:%M:%SZ"):
    """
    Convert the results from a postgres table into a dict
    :param results: list of items straight from the database
    :return: list of json docs
    """
    docs = []
    for doc_id, author, version, creationDate, modificationDate, group, jsonb in results:
        doc = {
            "#id": doc_id,
            "#author": author,
            "#version": version,
            "#creationDate": creationDate.strftime(time_format),
            "#modificationDate": modificationDate.strftime(time_format),
            "#group": group
        }

        if isinstance(group, type(None)):
            doc["#group"] = ""  # make sure group is string

        doc.update(jsonb)
        docs.append(doc)

    return docs

mmapi_collection_names = ["sensors", "stations", "variables", "qualityControl", "people", "units", "processes",
                                 "organizations", "datasets", "operations", "activities", "projects", "resources",
                                 "programmes"]

class MetadataCollector(LoggerSuperclass):
    def __init__(self, connection: dict, default_author: str, organization: str, log: logging.Logger):
        """
        Initializes a connection to a PostgresQL database hosting metadata
        :param connection: connection string
        :param default_author: default author (must be a people #id)
        :param organization: organization that owns the infrastructure (must be an organizations #id)
        """

        # To create new collections, add them here
        self.collection_names = mmapi_collection_names

        LoggerSuperclass.__init__(self, log, "MC", PRL)
        self.info("Initializing MetadataCollector")

        self.dataset_registry_table = "dataset_registry"

        self.default_author = default_author
        self.organization = organization
        self.__connection_chain = connection
        self.__taxa_aphia_dict = {}

        host = connection["db_host"]

        if host == os.uname().nodename:
            host = "localhost"

        port = connection["db_port"]
        db_name = connection["db_name"]
        self.db_name = db_name
        self.db_hist_name = db_name + "_hist"
        db_user = connection["db_user"]
        db_password = connection["db_password"]
        log = logging.getLogger()
        try:
            self.info(f"Connecting to database '{db_name}'...")
            self.db = PgDatabaseConnector(host, port, db_name, db_user, db_password, log)
            self.db_hist = PgDatabaseConnector(host, port, self.db_hist_name, db_user, db_password, log,
                                               autocommit=True)
            self.info("Database ok")
        except psycopg2.OperationalError:
            self.info(f"Database not initialized! creating '{db_name}'")
            # Probably database does not exist! initialize databases
            # Connect to the user database
            self.db = PgDatabaseConnector(host, port, db_user, db_user, db_password, log, autocommit=True)
            self.__init_database()
            self.db.close()
            self.db = PgDatabaseConnector(host, port, db_name, db_user, db_password, log)
            self.db_hist = PgDatabaseConnector(host, port, self.db_hist_name, db_user, db_password, log)
        self.__init_tables()
        self.info("Database initialized")

        self.metadata_schema = mmm_metadata  # JSON schema for
        self.schemas = mmm_schemas

        # The cache stores in memory documents already retrieved from the database, this will significantly speed up
        # the system and reduce the database workload
        self.__cache_timeout_s = 300  # 5 minutes
        self.__cache = {}
        self.__sensor_deployment_cache = {}
        self.used_time = 0

    def __init_database(self):
        """
        Creates a documents table
        :return:
        """
        if not self.db.check_if_database_exists(self.db_name):
            self.info(f"Creating database {self.db_name}")
            self.db.exec_query(f"create database {self.db_name};", fetch=False)

        if not self.db.check_if_database_exists(self.db_hist_name):
            self.info(f"Creating database {self.db_hist_name}")
            self.db.exec_query(f"create database {self.db_hist_name};", fetch=False)

    def __init_tables(self):
        """
        Create database tables
        """
        table_names = self.db.list_from_query("SELECT table_name FROM information_schema.tables")
        table_names_hist = self.db_hist.list_from_query("SELECT table_name FROM information_schema.tables")

        for collection in self.collection_names:
            collection = collection.lower()  # use lowercase in SQL
            if collection not in table_names:
                self.info(f"   Creating table {collection}")
                query = f"""
                CREATE TABLE {collection} (
                    doc_id VARCHAR(255) PRIMARY KEY,                
                    author VARCHAR(255),
                    doc_version SMALLINT,
                    creationDate TIMESTAMPTZ,
                    modificationDate TIMESTAMPTZ,
                    doc_group VARCHAR(255),
                    doc JSONB
                );
                """
                self.db.exec_query(query, fetch=False)

            # Now add dataset registry
            # dataset_id: ID of the dataset
            # resource_id: ID of the resource
            # service: Service (e.g. fileserver, erddap...)
            # format: file format (csv, netcdf, zip...)
            # data_from: time period start (e.g. 2020-01-01). It is not the same as the data point start
            # data_to  time period end (e.g. 2021-01-01). It is not the same as the data point start
            # path: path to the file in the server
            # host: host of the server
            # url: public link to the file, if exists
            # creation_date: creation date of the resource
            # modification_date: modification date of the resource
            # time_min: first           ervation timestamp in the file
            # time_max: last observation timestamp in the file
            # lat_min: min latitude
            # lat_max: max latitude
            # lon_min: min longitude
            # lon_max: max longitude
            # depth_min: min depth
            # depth_max: max depth
            # md5: file md5 hash

        if self.dataset_registry_table not in table_names:
            query = f"""
            CREATE TABLE IF NOT EXISTS {self.dataset_registry_table}
                (
                    dataset_id TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    service TEXT NOT NULL,
                    format TEXT NOT NULL,
                    data_from timestamp with time zone,
                    data_to timestamp with time zone,                    
                    path TEXT not NULL,    
                    host TEXT not NULL,
                    url TEXT,
                    creation_date timestamp with time zone NOT NULL,
                    modification_date timestamp with time zone NOT NULL,                    
                    time_min  timestamp with time zone NOT NULL,
                    time_max timestamp with time zone NOT NULL,            
                    lat_min float NOT NULL,
                    lat_max float NOT NULL,
                    lon_min float NOT NULL,
                    lon_max float NOT NULL,
                    depth_min float NOT NULL,
                    depth_max float NOT NULL,
                    md5 text NOT NULL,
                    doi TEXT,
                    zenodo_record TEXT
                )
            """
            self.db.exec_query(query, fetch=False)
            self.db.exec_query(
                f"""ALTER TABLE {self.dataset_registry_table} 
                           ADD UNIQUE (dataset_id, resource_id, service, format, data_from, data_to);""",
                fetch=False
            )

        # History database
        for collection in self.collection_names:
            collection = collection.lower()  # use lowercase in SQL
            if collection not in table_names_hist:
                self.info(f"Creating table {collection}")
                query = f"""
                 CREATE TABLE {collection} (
                     doc_id VARCHAR(255),                
                     author VARCHAR(255),
                     doc_version SMALLINT,
                     creationDate TIMESTAMPTZ,
                     modificationDate TIMESTAMPTZ,
                     doc_group VARCHAR(255),
                     doc JSONB
                 );
                 """
                self.db_hist.exec_query(query, fetch=False)
                query = (f"alter table {collection} add constraint {collection}_id_version_unique unique"
                         f" (doc_id, doc_version);")
                self.db_hist.exec_query(query, fetch=False)

    def __add_to_cache(self, collection, doc):
        """
        Adds a document to the cache
        :param collection:  collection
        :param doc: document to add
        :return:
        """
        doc_id = doc["#id"]
        if collection not in self.__cache.keys():
            self.__cache[collection] = {}

        self.__cache[collection][doc_id] = (time.time(), doc)

    def __get_from_cache(self, collection, doc_id):
        """
        Get a document from the cache
        :param collection:
        :param doc:
        :return: the document or None if the document is not on the cache (or timeout has expired)
        """
        if collection not in self.__cache.keys():
            return None  # Collection not found
        elif doc_id not in self.__cache[collection].keys():
            return None  # Document not found
        # get the document
        timestamp, doc = self.__cache[collection][doc_id]
        # check the timeout condition
        if time.time() - timestamp > self.__cache_timeout_s:
            del self.__cache[collection][doc_id]
            return None
        return doc

    def validate_document(self, doc: dict, collection: str, exception=True, metadata=True):
        """
        This method takes a document and checks if it is valid. A document should at least contain the following fields
            #id: (string) id of the document
            #version: (int) version of the document
            #creationDate: (string) date of the first version of the document
            #modificationDate: (string) date with the latest version
        :param doc: JSON dict with document data
        :param collection: collection name
        :param exception: Wether to throw and exception on error or not
        :param metadata: validate the metadata or not (elements starting with #)
        :return: Nothing
        :raise: SyntaxError
        """
        errors = []
        if metadata:
            errors = validate_schema(doc, mmm_metadata, errors=errors, strip_meta=False)
        if collection not in mmm_schemas.keys():
            self.warning(f"WARNING: no schema for '{collection}'")
        else:
            try:
                errors = validate_schema(doc, mmm_schemas[collection], errors=errors)
            except jsonschema.exceptions.SchemaError as e:
                self.error(f"ERROR validating document '{doc['#id']}' with schema '{collection}'")
                raise e
        if errors:
            for e in errors:
                self.error(f"{e}")
            if exception:
                self.error(json.dumps(doc, indent=2))
                raise ValueError(f"Document not valid: {str(errors)}")
            return False  # return false if exception=False
        else:
            return True  # document is valid


    def get_identifiers(self, collection, history=False):
        """
        Get a list of all ids within a collection
        :param collection: collection name
        :return: list of ids
        """
        return self.db.list_from_query(f"select doc_id from {collection.lower()};")

    def get_documents(self, collection: str, filter="", history=False) -> list:
        """
        Return all documents in a collection
        :param collection: collectio name
        :param filter: sql option to add at the query, like "id = 'myid' limit 1"
        :param history: search in archived documents
        :return: list of documents that match the criteria
        """
        if collection not in self.collection_names:
            raise LookupError(f"Collection {collection} not found!")

        query = f"select doc_id, author, doc_version, creationdate, modificationdate, doc_group, doc from {collection.lower()}"

        if filter:
            query += f" {filter}"
        query += ";"

        if not history:
            results = self.db.list_from_query(query)
        else:
            results = self.db_hist.list_from_query(query)
        docs = postgres_results_to_dict(results)
        if not history:
            for doc in docs:
                self.__add_to_cache(collection, doc)
        return docs

    # --------- Document Operations --------- #
    def insert_document(self, collection: str, document: dict, author: str = "", force=False, update=False):
        """
        Adds metadata to a document and then inserts it to a collection.
        :param collection: collection name
        :param document: json doc to be inserted
        :param author: people #id of the author (if not set the default author will be set)
        :param force: insert even if the document fails the validation
        :param update: if set update the document if a previous version existed
        :return: document with metadata
        """
        # first check that the doc's #id is not already registered

        try:
            author = document["#author"]
        except KeyError:
            author = self.default_author

        if collection not in self.collection_names:
            raise ValueError(f"Collection {collection} not valid!")

        if document["#id"] in self.get_identifiers(collection):
            if update:
                self.warning(f"Document '{document['#id']}' already exists! udpating")
                return self.replace_document(collection, document["#id"], document, force=force)
            else:
                raise NameError(f"{collection} document with id {document['#id']} already exists!")

        # Check if there's an historical version
        document_id = document["#id"]
        self.debug(f"Checking if there are historical verisons for '{collection}:{document_id}'")
        q = (f"select doc_version from {collection.lower()} where doc_id = '{document_id}' order by doc_version desc"
             f" limit 1;")
        versions = self.db_hist.list_from_query(q)
        if len(versions) > 0:
            self.debug(f"historical version {versions[0]}")
            version = versions[0] + 1
        else:
            version = 1
            self.debug(f" no historical, setting v=0")

        now = get_timestamp_string()
        self.validate_document(document, collection, exception=(not force), metadata=False)

        document["#version"] = version
        document["#creationDate"] = now
        document["#modificationDate"] = now
        document["#author"] = author
        if "#group" not in document.keys():
            document["#group"] = ""
        self.debug(f"Inserting {document_id} to {collection.lower()}")
        contents = strip_metadata_fields(document)
        insert_query = sql.SQL(f"""
            INSERT INTO {collection.lower()} (doc_id, author, doc_version, creationDate, modificationDate, doc)
            VALUES (%s, %s, %s, %s, %s, %s)
        """)
        values = (document_id, author, document["#version"], document["#creationDate"], document["#modificationDate"],
                  json.dumps(contents))

        self.db.exec_query((insert_query, values), fetch=False)
        self.insert_document_history(collection, document)
        return document

    def insert_document_history(self, collection: str, document: dict, author: str = ""):
        if collection not in self.collection_names:
            raise ValueError(f"Collection {collection} not valid!")
        self.validate_document(document, collection, exception=True)
        document_id = document["#id"]
        version = document["#version"]
        author = document["#author"]
        creation_date = document["#creationDate"]
        modification_date = document["#modificationDate"]
        group = document["#group"]

        contents = strip_metadata_fields(document)
        insert_query = sql.SQL(f"""
            INSERT INTO {collection.lower()} (doc_id, author, doc_version, creationDate, modificationDate, doc, doc_group)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """)
        values = (document_id, author, version, creation_date, modification_date, json.dumps(contents), group)
        self.db_hist.exec_query((insert_query, values), fetch=False)
        return document

    def exists(self, collection, document_id):
        try:
            self.get_document(collection, document_id)
            return True
        except LookupError:
            return False

    def get_document(self, collection: str, document_id: str, version: int = 0):
        """
        Gets a single document from a collection. If version is used a specific version in the historical database
        will be fetched

        :param collection: name of the collection
        :param document_id: id of the document
        :param version: version (int)
        """
        if not version:
            docs = self.get_documents(collection, filter=f"where doc_id = '{document_id}'")

        else:
            docs = self.get_documents(collection, filter=f"where doc_id = '{document_id}' and doc_version = {version}",
                                      history=True)

        if len(docs) > 1:
            self.error(f"Expected only one document with id={document_id}, but database returned {len(docs)}!",
                       exception=True)
        elif len(docs) == 0:
            self.error(f"Document '{document_id}' not found in collection '{collection}'", exception=LookupError)
        return docs[0]

    def get_document_history(self, collection, document_id):
        """
        Looks for all versions of a document in the history database and returns them all.
        """
        return self.get_documents(collection, filter=f"where doc_id = '{document_id}' order by doc_version desc",
                                  history=True)

    def replace_document(self, collection: str, document_id: str, document: dict, author=False, force=False):
        """
        Takes a document in the database, updates the metadata and adds new info. Metadata (#version, #modificationDate,
        etc.) are modified automatically. All other fields are replaced by the fields in the input document.
        :param document_id: #id
        :param collection: collection name
        :param document: elements to update
        :param author: author of the document
        :param force: If true, ignore metadata checks and insert document
        """

        if document["#id"] != document_id:
            raise ValueError("Document #id does not match with parameter id")

        try:
            author = document["#author"]
        except KeyError:
            author = self.default_author

        old_document = self.get_document(collection, document_id)  # getting old metadata

        old_group = old_document["#group"]
        new_group = document.get("#group", "")

        metadata = {key: value for key, value in old_document.items() if key.startswith("#")}
        metadata["#version"] += 1
        metadata["#modificationDate"] = get_timestamp_string()
        metadata["#author"] = author  # update author
        metadata["#creationDate"] = old_document["#creationDate"]
        self.debug(f"Replace {document_id} to {collection}, new version={metadata['#version']}")

        old_contents = {key: value for key, value in old_document.items() if not key.startswith("#")}

        # keep only elements that are not metadata
        contents = strip_metadata_fields(document)
        new_document = metadata  # start new document with metadata
        new_document.update(contents)  # add contents after metadata

        if old_group != new_group:
            new_document["#group"] = new_group

        if contents == old_contents and old_group == new_group:
            if force:
                self.warning(f"document {document['#id']} is identical to previous one")
            else:
                self.warning(f"old and new documents are equal for {document['#id']}, ignoring")
                return new_document

        self.validate_document(new_document, collection, exception=(not force), metadata=False)

        # Define the update query
        query = sql.SQL(f"""
            UPDATE {collection.lower()}
            SET author = %s,
                doc_version = %s,                
                modificationdate = %s,
                doc_group = %s,
                doc = %s
            WHERE doc_id = '{document_id}';
        """)

        # Data to update
        new_data = (
            author,
            metadata["#version"],
            metadata["#modificationDate"],
            metadata["#group"],
            json.dumps(contents),
        )
        self.db.exec_query((query, new_data), fetch=False)

        # Now add it to history
        self.insert_document_history(collection, new_document)
        return new_document

    def delete_document(self, collection: str, document_id: str, history=False):
        """
        drops an element in collection
        :param document_id: #id
        :param collection: collection name
        :param history: if True delete also all history elements
        """
        self.debug(f"Deleting {document_id} from {collection.lower()}")
        query = f"delete from {collection.lower()} where doc_id = '{document_id}';"
        self.db.exec_query(query, fetch=False)
        if history:
            self.db_hist.exec_query(query, fetch=False)

    # --------- Wrappers for collections --------- #
    def get_sensor(self, identifier):
        return self.get_document("sensors", identifier)

    def get_variable(self, identifier):
        return self.get_document("variables", identifier)

    def get_station(self, identifier):
        return self.get_document("stations", identifier)

    def get_unit(self, identifier):
        return self.get_document("units_old", identifier)

    def get_quality_control(self, identifier, qartod_only=False):
        conf = self.get_document("qualityControl", identifier)
        if qartod_only:  # return only the artod field
            return {"qartod": conf["qartod"]}

    def get_people(self, identifier):
        return self.get_document("people", identifier)

    def get_organization(self, identifier):
        return self.get_document("organizations", identifier)

    def get_qc_from_sensor(self, sensor, qartod_only=False):
        """
        Takes all QC configurations from a sensor and merges it to a single dict
        :param sensor: sensor id
        :param qartod_only: is set only the qartod field is returned, useful to use directly in QC scripts
        :return: dict with all qc config
        """
        sensor = self.get_sensor(sensor)
        qc = {}
        for variable in sensor["variables"]:
            if "@qualityControl" in variable.keys():
                varconfig = self.get_quality_control(variable["@qualityControl"], qartod_only=qartod_only)
                qc[variable["@variables"]] = varconfig
        return qc

    def get_sensor_variables(self, sensor_id):
        """
        Returns a dict with all the sensor variables
        :param sensor_id:
        :return: A dict with all the variables {"var1": { ... }, "VAR2": { ...}}
        """
        variables = {}
        sensor = self.get_sensor(sensor_id)
        for variable in sensor["variables"]:
            variable_id = variable["@variables"]
            variables[variable_id] = self.get_variable(variable_id)
        return variables

    def get_polar_variables(self, sensor_id):
        """
        Returns two list with the modules and angles variables in a dataset. Both lists have the same size
        :param sensor_id: sensor identifier
        :return: two lists with same size: [module_list, angle_list]
        """
        variables = self.get_sensor_variables(sensor_id)
        modules = []
        angles = []
        for var in variables.values():
            if "polar" in var.keys() and var["polar"]["module"] == var["#id"]:
                modules.append(var["polar"]["module"])
                angles.append(var["polar"]["angle"])
        return modules, angles

    def get_log_variables(self, sensor_id):
        """
        Returns a list of all the variables that are
        :param sensor_id:
        :return: two lists with same size: [module_list, angle_list]
        """
        variables = self.get_sensor_variables(sensor_id)
        return [identifier for identifier, var in variables.items() if
                "logarithmic" in var.keys() and var["logarithmic"]]

    def get_no_average_variables(self, sensor_id):
        """
        Returns a list of all the sensor variables that have averaging disabled
        :param sensor_id: sensor identifier
        :return: two lists with same size: [module_list, angle_list]
        """
        variables = self.get_sensor_variables(sensor_id)
        return [identifier for identifier, var in variables.items() if "average" in var.keys() and not var["average"]]

    def get_people_from_role(self, sensor_id, role):
        """
        Returns a person instance based on their role on a sensor
        :param sensor_id:
        :param role: role to search
        :return:
        """

        for contact in self.get_sensor(sensor_id)["contacts"]:
            if contact["role"] == role:
                return self.get_people(contact["@people"])

        raise LookupError(f"Person with role {role} not found")

    def get_organization_from_role(self, sensor_id, role):
        """
        Returns a person instance based on their role on a sensor
        :param sensor_id:
        :param role: role to search
        :return:
        """
        for contact in self.get_sensor(sensor_id)["contacts"]:
            if contact["role"] == role:
                return self.get_organization(contact["@organizations"])
        raise LookupError(f"Organizations with role {role} not found")

    def reset_version_history(self):
        """
        USE WITH CAUTION! This method will
            1. Drop history database
            2. Reset all versions in database to v=1
            3. Copy all documents from database to history database
        :return: Nothing
        """
        raise ValueError("Unimplemented")

    def get_contact_by_role(self, doc: dict, role: str) -> Tuple[dict, str]:
        """
        Loops through the contacts section in a document and returns the id and the collection type (organization or
        people) of the first contact that has a certain role.
        :param doc:
        :param role:
        :return: doc, collection
        """

        if "contacts" not in doc.keys():
            raise LookupError(f"Document with #id={doc['#id']} does not have contacts!")

        for contact in doc["contacts"]:
            if contact["role"] == role:
                if "@people" in contact.keys():
                    return self.get_document("people", contact["@people"]), "people"
                elif "@organizations" in contact.keys():
                    return self.get_document("organizations", contact["@organizations"]), "organizations"
                else:
                    raise ValueError("Contact type not valid!")
        raise LookupError(f"Contact with role '{role}' not found in document '{doc['#id']}'")

    def get_affiliation(self, doc: str|dict, t:pd.Timestamp=None):
         """
         Returns the latest affiliation of an individual (person document passed in doc). If t (time) is supplied,
         the affiliation that matches time is supplied. If multiple affiliations are found at the same time, the first
         is returned.
         :param doc: document ID (str) or document (dict) containing th person's info
         :param t: p.Timatmp
         :return: organization document_id or Lookup error if not found
         """
         assert_types(doc, [str, dict])
         assert_types(t, [pd.Timestamp, type(None)])
         if isinstance(doc, str):
             doc = self.get_document("people", doc)

         if "affiliations" not in doc.keys() or not doc["affiliations"]:
             raise LookupError(f"Person '{doc['#id']}' has no affiliations")

         affiliations = doc["affiliations"]

         if t is not None:
             if t.tz is None:
                 t = t.tz_localize("UTC")

             for affiliation in affiliations:
                 start = affiliation.get("start", "")
                 end = affiliation.get("end", "")

                 if not start:
                     start = pd.Timestamp.min.tz_localize("UTC")
                 else:
                     start = pd.Timestamp(start)
                     if start.tz is None:
                         start = start.tz_localize("UTC")

                 if not end:
                     end = pd.Timestamp.max.tz_localize("UTC")
                 else:
                     end = pd.Timestamp(end)
                     if end.tz is None:
                         end = end.tz_localize("UTC")

                 if start <= t <= end:
                     return affiliation["@organizations"]

             raise LookupError(f"Affiliation for person '{doc['#id']}' at time {t} not found")

         latest_affiliation = None
         latest_start = None

         for affiliation in affiliations:
             start = affiliation.get("start", "")
             if not start:
                 start = pd.Timestamp.min.tz_localize("UTC")
             else:
                 start = pd.Timestamp(start)
                 if start.tz is None:
                     start = start.tz_localize("UTC")

             if latest_start is None or start > latest_start:
                 latest_start = start
                 latest_affiliation = affiliation

         if latest_affiliation is None:
             raise LookupError(f"Affiliation for person '{doc['#id']}' not found")

         return latest_affiliation["@organizations"]
    def __check_link(self, parent_collection: str, parent_doc_id: str, target_collection: str, target_doc: str,
                     errors: list) -> list:
        """
        Checks if the document which a link is pointing really exists, ensuring the correctness of the link itself,

        :param parent_collection: Collection of the document being analyzed
        :param parent_doc_id: document ID
        :param target_collection: collection where the link points
        :param target_doc: document ID where the link points
        :param errors: list with all errors as string
        :return: error list with new errors
        """
        try:
            d = self.get_document(target_collection, target_doc)
            if not d:
                raise ValueError("Never null!")
        except LookupError:
            errors.append(f"{parent_collection}:'{parent_doc_id}' broken link {target_collection}:'{target_doc}'")
        return errors

    def __check_dict(self, collection: str, doc_id: str, doc: dict, errors: list) -> list:
        """
        Look for links within a document or document exceropt. If found, ensure that those links are correct
        :param collection: collectio name
        :param doc_id:
        :param doc: document or document excerpt
        :param errors: list of errors where new errors will be appended
        :return: errors
        """
        for key, value in doc.items():
            if type(key) != str:
                raise ValueError(f"Keys must be strings! Error when analyzing {doc_id} from collection {collection}")

            # Ensure links
            if key.startswith("@"):
                if type(value) == str:
                    errors = self.__check_link(collection, doc_id, key[1:], value, errors)
                elif type(value) == list:
                    for val in value:
                        errors = self.__check_link(collection, doc_id, key[1:], val, errors)
                else:
                    raise ValueError(f"Wrong type in {doc_id} {key}: value type {type(value)}")

            # Process other objects
            elif type(value) == dict:
                errors = self.__check_dict(collection, doc_id, value, errors)
            elif type(value) == list:
                for subvalue in value:
                    if type(subvalue) == dict:
                        errors = self.__check_dict(collection, doc_id, subvalue, errors)
        return errors

    def __warning(self, collection, doc, warnings):
        """
        Hardcoded warnings
        """
        if collection == "sensors":
            if "deployment" in doc.keys():
                w = f"{collection}:{doc['#id']} 'deployment' in Sensors is deprecated!"
                rich.print(f"[yellow]{w}")
                warnings.append(w)

            if "dataType" in doc.keys():
                if "dataSource" in doc.keys():
                    w = f"{collection}:{doc['#id']} 'dataSource' in datasets root will be ignored!"
                    rich.print(f"[yellow]{w}")
                    warnings.append(w)

            # Check deployments
            deps = self.get_sensor_deployments(doc["#id"])
            if len(deps) < 1:
                w = f"{collection}:{doc['#id']} doesn't have any deployment!"
                rich.print(f"[yellow]{w}")
                warnings.append(w)

        if collection == "datasets":
            if "export" in doc.keys():
                wrong_export_keys = ["host", "periodicity", "period", "host"]
                for k in wrong_export_keys:
                    if k in doc["export"].keys():
                        w = f"{collection}:{doc['#id']} includes wrong key '{k}'"
                        warnings.append(w)
        return warnings

    def healthcheck(self, collections=None):
        """
        Ensure all relations in the database. For every document validate it against the generic schema (metadata
        schema), collection schema and scan the document for broken relation (@-fields).
        """
        init = time.time()
        if collections is None:
            collections = []
        assert (type(collections) is list)

        errors = []
        warnings = []
        if not collections:
            collections = self.collection_names

        arguments = []
        for col in collections:
            docs = self.get_documents(col)
            for doc in docs:
                arguments.append([doc, col])
                doc_errors, doc_warnings = self.validate_doc(doc, col)
                warnings += doc_warnings
                errors += doc_errors

        if warnings:
            self.info("Warning report")
            [self.warning(f"  {warning}") for warning in warnings]
            self.warning(f"Got {len(warnings)} warnings!")

        if errors:
            self.info("Error report")
            [self.error(f"  {error}") for error in errors]
            self.error(f"[red]Got {len(errors)} errors!")
        else:
            self.info(f"  =) =) Congratulations! You have a healthy database (= (=\n")
            self.debug(f"healthcheck took {time.time() - init:.02f} seconds")

    def validate_doc(self, doc, collection):
        errors = []  # list of document errors
        try:
            schema = mmm_schemas[collection]
        except KeyError:
            schema = {}
        if schema:
            errors = validate_schema(doc, schema, [], verbose=True)

        errors = self.__check_link(collection, doc["#id"], "people", doc["#author"], errors)
        # Scan the rest of the document and check its relations
        errors = self.__check_dict(collection, doc["#id"], doc, errors)
        warnings = self.__warning(collection, doc, [])
        return errors, warnings

    def get_station_position(self, station_name: str, timestamp: pd.Timestamp = None) -> Tuple[float, float, float]:
        """
        Returns (latitude, longitude, depth) for a station at a particular time. It looks for all deployments of a
        station and selects the one immediately before the selected time. If timestamp is null return the last
        deployment
        """
        assert_types(timestamp, [pd.Timestamp, type(None)])
        self.debug(f"Getting activities applied to {station_name}")
        sql_filter = f" where doc->>'type' = 'deployment' and doc->'appliedTo'->>'@stations' = '{station_name}'"
        hist = self.get_documents("activities", sql_filter)
        self.debug(f"Got {len(hist)} activities")
        data = {
            "time": [],
            "latitude": [],
            "longitude": [],
            "depth": [],
        }
        for dep in hist:
            data["time"].append(dep["time"])
            data["latitude"].append(round(dep["where"]["position"]["latitude"], 6))
            data["longitude"].append(round(dep["where"]["position"]["longitude"], 6))
            data["depth"].append(round(dep["where"]["position"]["depth"], 2))

        self.debug(f"Creating dataframe with deployments")
        df = pd.DataFrame(data)
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.set_index("time")
        df = df.sort_index(ascending=False)
        df = df.reset_index()

        self.debug(f"Loking for deployment previous to {timestamp}")

        if type(timestamp) == type(None):
            # If timestamp not specified return the last deployment
            return float(df["latitude"].values[0]), float(df["longitude"].values[0]), float(df["depth"].values[0])

        if timestamp.tz is None:
            timestamp = timestamp.tz_localize("UTC")

        # Force 00:00:00
        timestamp = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)

        # Loop backwards in deployments to get the first deployment before the timestamp
        for idx, row in df.iterrows():
            if timestamp >= row["time"].replace(hour=0, minute=0, second=0, microsecond=0):
                self.debug(f'Found deployement lat={row["latitude"]} lon={row["longitude"]} depth={row["depth"]}')
                return float(row["latitude"]), float(row["longitude"]), float(row["depth"])

        raise LookupError(
            f"Deployment for station={station_name} before {timestamp} not found, only found={data['time']}")

    def drop_all(self):
        """
        Deletes ALL documents from ALL collections, USE WITH CAUTION!
        """
        for col in self.collection_names:
            docs = self.get_documents(col)
            for doc in docs:
                self.delete_document(col, doc["#id"], history=True)

        self.db.exec_query(f"delete from {self.dataset_registry_table};", fetch=False)

    def get_last_sensor_deployment(self, sensor_id) -> Tuple[str, pd.Timestamp, bool]:
        """
        Returns the name of the last station where this sensor was deployed
        :return: station_id (str), deployment_time (pd.Timestamp), is_active (bool)
        """
        assert_type(sensor_id, str)
        doc = self.db.value_from_query(
            f"""
            select doc from activities 
            where doc->>'type' = 'deployment' and doc->'appliedTo'->>'@sensors' = '{sensor_id}' and doc->>'status' = 'done' 
            order by (doc->>'time')::timestamp desc
            limit 1
            """
        )
        deployment_t = pd.Timestamp(doc["time"])

        try:
            recovery_t = self.get_last_sensor_recovery(sensor_id)
        except LookupError:
            # No recovery for this sensor
            active = True
            return doc["where"]["@stations"], deployment_t, active

        if deployment_t > recovery_t:
            active = True
        else:
            active = False
        return doc["where"]["@stations"], deployment_t, active

    def get_last_sensor_recovery(self, sensor_id) -> pd.Timestamp:
        """
        Returns the timestamp of the last recovery (or loss)
        :param sensor_id:
        :return:
        """
        assert_type(sensor_id, str)
        doc = self.db.value_from_query(
            f"""
            select doc from activities 
            where doc->>'type' in ('recovery', 'loss') and doc->'appliedTo'->>'@sensors' = '{sensor_id}'
            order by (doc->>'time')::timestamp desc
            limit 1
            """, debug=False
        )
        return pd.Timestamp(doc["time"])

    def __get_deployments(self, element_type, identifier) -> list:
        # Get all activities and involving this station
        sql_filter = f"where doc->'appliedTo'->>'@{element_type}' = '{identifier}'"
        hist = self.get_documents("activities", filter=sql_filter)

        act_deployments = [d for d in hist if d["type"] == "deployment"]  # keep all deployments
        act_recoveries = [d for d in hist if d["type"] in ["recovery", "loss"]]  # keep all recoveries/losses
        if len(act_deployments) == 0:
            raise LookupError(f"No deployments found for {element_type} {identifier}")

        recover_dates = [pd.to_datetime(r["time"]) for r in act_recoveries]  # Keep only the dates
        recover_dates = sorted(recover_dates)

        deployments = []
        for d in act_deployments:
            deployment = {
                "id": d["#id"],
                "start": pd.to_datetime(d["time"]),
                "end": None,
            }
            if "position" in d["where"].keys():
                deployment["coordinates"] = d["where"]["position"]
            if "@stations" in d["where"].keys():
                deployment["station"] = d["where"]["@stations"]

            if "fieldOfView" in d.keys():
                deployment["fieldOfView"] = d["fieldOfView"]

            deployments.append(deployment)

        deployments = sorted(deployments, key=lambda x: x["start"])

        for i in range(len(deployments)):
            deployment = deployments[i]
            deployment_end_candidates = []

            # recovery upper limit
            upper_limit = pd.to_datetime("2100-01-01", utc=True)
            if i == len(deployments) - 1:
                # This is the last element, no further deployments
                pass
            else:
                upper_limit = pd.to_datetime(deployments[i + 1]["start"])
                deployment_end_candidates.append(upper_limit)

            for timestamp in recover_dates:
                # Get the timestamp
                if deployment["start"] < timestamp <= upper_limit:
                    deployment_end_candidates.append(timestamp)

            if len(deployment_end_candidates) > 0:
                deployment["end"] = min(deployment_end_candidates)  # The first candidate is the valid recovery time
        return deployments

    def get_sensor_deployments(self, sensor: dict | str, interval=()) -> list:
        """
        Returns a list of dicts with the info of each deployment registered in the sensor.
            [
                {
                    "start": pd.Timestamp,
                    "end": pd.Timestamp|None,
                    "station": station_id
                    "coordinates": { "latitude": XX, "longitude": YY, "depth": ZZ }
                },
                ...
            ]

        If interval is passed, only the deployments within the interval are returned
        """
        assert_types(sensor, [str, dict])
        if type(sensor) is str:
            sensor_id = sensor
        elif type(sensor) is dict:
            sensor_id = sensor["#id"]
            pass
        else:
            raise ValueError(f"Wrong type in station, expected str or dict, got {type(sensor)}")

        if sensor_id in self.__sensor_deployment_cache.keys():
            deployments, t = self.__sensor_deployment_cache[sensor_id]
            if time.time() - t < self.__cache_timeout_s:
                return deployments

        try:
            deployments = self.__get_deployments("sensors", sensor_id)
        except LookupError:
            self.warning(f"No deployments for sensor {sensor_id}")
            return []

        for deployment in deployments:
            station_id = deployment["station"]
            latitude, longitude, depth = self.get_station_coordinates(station_id, timestamp=deployment["start"])
            deployment["coordinates"] = {"latitude": latitude, "longitude": longitude, "depth": depth}

        if interval:
            valid_start, valid_end = interval
            assert_type(valid_start, pd.Timestamp)
            assert_type(valid_end, pd.Timestamp)
            deployments_inside_interval = []
            for dep in deployments:
                if dep["start"] <= valid_start:
                    if isinstance(dep["end"], pd.Timestamp):
                        if dep["end"] >= valid_end:
                            deployments_inside_interval.append(dep)
                    elif isinstance(dep["end"], type(None)):
                        deployments_inside_interval.append(dep)
            deployments = deployments_inside_interval

        self.__sensor_deployment_cache[sensor_id] = (deployments, time.time())
        return deployments

    def get_sensor_deployment(self, sensor: dict | str, timestamp: pd.Timestamp) -> Tuple[float, float, float, dict]:
        """Gets the sensor deployment at a certain moment in time

        return latitude, longitude, depth and a dict with options like "fieldOfView"
        """
        deployments = self.get_sensor_deployments(sensor)

        if len(deployments) != 1:
            raise ValueError("Unimplemented! Expected only one sensor deployment")
        d = deployments[0]["coordinates"]
        return d["latitude"], d["longitude"], d["depth"]

    def get_station_deployments(self, station: dict | str) -> list:
        """
        Returns a list of dicts with the info of each deployment registered in the station:
            [
                {
                    "start": pd.Timestamp,
                    "end": pd.Timestamp|None,
                    "coordinates": { "latitude": XX, "longitude": YY, "depth": ZZ }
                    "sensors": list
                },
                ...
            ]
        """
        assert_types(station, [str, dict])
        if type(station) is str:
            station = self.get_document("stations", station)
        elif type(station) is dict:
            pass
        else:
            raise ValueError(f"Wrong type in station, expected str or dict, got {type(station)}")

        station_id = station["#id"]
        deployments = self.__get_deployments("stations", station_id)

        # Get all sensors that have been deployed into this platform
        deployed_sensors = self.db.dataframe_from_query(f"""
            select doc->'appliedTo'->'@sensors' as sensor, doc->>'time' as timestamp
            from activities
            where   doc->'where'->>'@stations' = '{station_id}'
            ;
        """)

        for d in deployments:
            d["sensors"] = []  # create an empty list of sensors
            start_time = d["start"]
            end_time = d["end"]
            if isinstance(end_time, type(None)):
                # Create date ridiculously in the future
                end_time = pd.to_datetime("2200-01-01T00:00:00Z")

            for _, row in deployed_sensors.iterrows():
                timestamp = pd.to_datetime(row["timestamp"])
                if start_time <= timestamp < end_time:
                    d["sensors"].append(row["sensor"])

        return deployments

    def get_station_coordinates(self, station: dict | str, timestamp=None) -> Tuple[float, float, float]:
        """
        Looks for the latest coordinates of a station based on its deployment history. Station may be station_id (str) or
        the station document (dict)
        :param mc: Metadata collector
        :param station: station or station:D
        :param timestamp: if set, get the coordinates in a specific timestamp
        :return latitude, longitude, depth
        """
        if type(station) is str:
            station = self.get_document("stations", station)
        elif type(station) is dict:
            pass
        else:
            raise ValueError(f"Wrong type in station, expected str or dict, got {type(station)}")

        assert_types(timestamp, [type(None), str, pd.Timestamp])

        deployments = self.get_station_deployments(station)

        if isinstance(timestamp, str):
            timestamp = pd.Timestamp(timestamp)

        if not timestamp:
            # Get the latest deployment
            latitude = deployments[-1]["coordinates"]["latitude"]
            longitude = deployments[-1]["coordinates"]["longitude"]
            depth = deployments[-1]["coordinates"]["depth"]
            return latitude, longitude, depth
        else:
            for deployment in reversed(deployments):
                if timestamp >= deployment["start"]:
                    latitude = deployment["coordinates"]["latitude"]
                    longitude = deployment["coordinates"]["longitude"]
                    depth = deployment["coordinates"]["depth"]
                    return latitude, longitude, depth

        station_id = station["#id"]
        raise LookupError(f"Station {station_id} coordinates not found! (timestamp={timestamp}")

    def get_station_history(self, name: str) -> list:
        """
        Looks for all activities with the
        """
        sql_filter = f" where doc->'appliedTo'->>'@stations' = '{name}'"
        activities = self.get_documents("activities", filter=sql_filter)
        history = []
        for a in activities:
            h = load_fields_from_dict(a, ["time", "type", "description", "where/position"],
                                      rename={"where/position": "position"})
            history.append(h)

        # Sort based on history
        history = sorted(history, key=lambda x: x['time'])
        return history

    def dataset_register(self,
                         dataset_id: str,
                         resource_id: str,
                         service: str,
                         fmt: str,
                         data_from: pd.Timestamp,
                         data_to: pd.Timestamp,
                         path: str,
                         host: str,
                         url: str,
                         time_min: pd.Timestamp,
                         time_max: pd.Timestamp,
                         lat_min: float,
                         lat_max: float,
                         lon_min: float,
                         lon_max: float,
                         depth_min: float,
                         depth_max: float,
                         md5: str,
                         doi: str = "",
                         zenodo_record: str = ""):
        """
        Register a dataset into the fileserver_dataset_registry.
        If no entry exists, insert it; otherwise update it.
        Empty strings for optional fields become NULL in the database.
        """
        # Type assertions (kept as-is)
        assert_type(dataset_id, str)
        assert_type(resource_id, str)
        assert_type(service, str)
        assert_type(fmt, str)
        assert_type(data_from, pd.Timestamp)
        assert_type(data_to, pd.Timestamp)
        assert_type(path, str)
        assert_type(host, str)
        assert_type(url, str)
        assert_type(time_min, pd.Timestamp)
        assert_type(time_max, pd.Timestamp)
        assert_type(lat_min, float)
        assert_type(lat_max, float)
        assert_type(lon_min, float)
        assert_type(lon_max, float)
        assert_type(depth_min, float)
        assert_type(depth_max, float)
        assert_type(md5, str)

        assert_type(doi, str)
        assert_type(zenodo_record, str)

        assert path, f"Empty path not valid!"

        # Helper: convert empty string to None (which becomes NULL in SQL)
        def empty_to_none(value):
            return None if value == "" else value

        now = pd.Timestamp.now(tz="UTC")

        self.debug(f"Registering dataset {dataset_id} {resource_id} {service}{fmt} {data_from} {data_to}")

        table_entry_exists = self.dataset_resource_exists(dataset_id, resource_id, service, fmt, data_from, data_to)

        if not table_entry_exists:  # INSERT
            self.info(
                f"CREATE dataset registry for dataset_id='{dataset_id}' and resource_id='{resource_id}' and service='{service}")
            query = f"""
                INSERT INTO {self.dataset_registry_table}
                (dataset_id, resource_id, service, format, data_from, data_to, path, host, url, creation_date, 
                modification_date, time_min, time_max, lat_min, lat_max, lon_min, lon_max, depth_min, depth_max, md5)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                dataset_id, resource_id, service, fmt, data_from,  data_to, path, host, empty_to_none(url), now, now,
                time_min, time_max, lat_min, lat_max, lon_min, lon_max, depth_min, depth_max, md5
            )
            self.db.exec_query((query,params), fetch=False)

        else:  # UPDATE
            self.info(f"UPDATE dataset registry for dataset_id='{dataset_id}' and resource_id='{resource_id}'")
            query = f"""
                UPDATE {self.dataset_registry_table}
                SET modification_date = %s,  path = %s, url = %s, host = %s,  time_min = %s, time_max = %s, 
                lat_min = %s, lat_max = %s, lon_min = %s, lon_max = %s, depth_min = %s, depth_max = %s, md5 = %s
                WHERE dataset_id = %s
                  AND resource_id = %s
                  AND service = %s
                  AND data_from = %s
                  AND data_to = %s
            """
            params = (
                now, path,  empty_to_none(url), host, time_min, time_max, lat_min, lat_max, lon_min, lon_max, depth_min,
                depth_max, md5,
                # WHERE condition
                dataset_id, resource_id, service, data_from, data_to
            )
            self.db.exec_query((query, params), fetch=False)

    def dataset_resource_exists(self, dataset_id: str, resource_id: str, service: str, fmt: str,
                                data_from: pd.Timestamp,
                                data_to: pd.Timestamp, ) -> bool:
        query = f"""
            SELECT EXISTS(
                SELECT 1 FROM {self.dataset_registry_table} 
                WHERE
                    dataset_id='{dataset_id}' 
                    and resource_id='{resource_id}' 
                    and service='{service}'
                    and format='{fmt}'
                    and data_from='{data_from}' 
                    and data_to='{data_to}'                    
            );"""
        return self.db.value_from_query(query)

    def update_zenodo_record(self, dataset_id: str, resource_id: str, service: str, data_from: List[pd.Timestamp], data_to: List[pd.Timestamp], zenodo_record: str):
        assert_type(dataset_id, str)
        assert_type(resource_id, str)
        assert_type(service, str)
        assert_type(data_from, list)
        assert_type(data_to, list)
        [assert_type(t, pd.Timestamp) for t in data_from]
        [assert_type(t, pd.Timestamp) for t in data_from]
        assert_type(zenodo_record, str)

        now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        data_from = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_from]
        data_to = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_to]
        self.info(f"UPDATE zenodo_record='{zenodo_record}'for dataset_id='{dataset_id}' and resource_id='{resource_id}' from {data_from} to {data_to}")

        query = f"""         
            UPDATE {self.dataset_registry_table}
            SET modification_date = '{now}', zenodo_record='{zenodo_record}' 
            WHERE
             dataset_id='{dataset_id}' and 
             resource_id='{resource_id}' and 
             service='{service}' and 
             data_from in {sql_list(data_from)} and 
             data_to in {sql_list(data_to)}
            ;"""

        self.db.exec_query(query, fetch=False)


    def update_gbif_record(self, dataset_id: str, resource_id: str, service: str, data_from: List[pd.Timestamp], data_to: List[pd.Timestamp], gbif_record: str):
        assert_type(dataset_id, str)
        assert_type(resource_id, str)
        assert_type(service, str)
        assert_type(data_from, list)
        assert_type(data_to, list)
        [assert_type(t, pd.Timestamp) for t in data_from]
        [assert_type(t, pd.Timestamp) for t in data_from]
        assert_type(gbif_record, str)

        now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        data_from = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_from]
        data_to = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_to]
        self.info(f"UPDATE gbif_record='{gbif_record}'for dataset_id='{dataset_id}' and resource_id='{resource_id}' from {data_from} to {data_to}")

        query = f"""         
            UPDATE {self.dataset_registry_table}
            SET modification_date = '{now}', gbif_record='{gbif_record}' 
            WHERE
             dataset_id='{dataset_id}' and 
             resource_id='{resource_id}' and 
             service='{service}' and 
             data_from in {sql_list(data_from)} and 
             data_to in {sql_list(data_to)}
            ;"""

        self.db.exec_query(query, fetch=False)


    def update_doi(self, dataset_id: str, resource_id: str, service: str, data_from: List[pd.Timestamp], data_to: List[pd.Timestamp], doi: str):
        assert_type(dataset_id, str)
        assert_type(resource_id, str)
        assert_type(service, str)
        assert_type(data_from, list)
        assert_type(data_to, list)
        [assert_type(t, pd.Timestamp) for t in data_from]
        [assert_type(t, pd.Timestamp) for t in data_from]
        assert_type(doi, str)

        assert doi, f"Invalid DOI: '{doi}'"

        now = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

        data_from = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_from]
        data_to = [t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in data_to]
        self.info(f"UPDATE DOI='{doi}'for dataset_id='{dataset_id}' and resource_id='{resource_id}' ")

        query = f"""         
            UPDATE {self.dataset_registry_table}
            SET modification_date = '{now}', doi='{doi}' 
            WHERE
             dataset_id='{dataset_id}' and 
             resource_id='{resource_id}' and 
             service='{service}' and 
             data_from in {sql_list(data_from)} and 
             data_to in {sql_list(data_to)}
            ;"""
        self.db.exec_query(query, fetch=False)


    def get_taxa_aphia_dict(self):
        """
        Looks all registered variables and stores taxa variables in a dictionary with key: scientific name, value: aphia ID
        :return: dict
        """
        if not self.__taxa_aphia_dict:
            for doc in self.get_documents("variables"):
                if "worms_id" in doc.keys():
                    self.__taxa_aphia_dict[doc["standard_name"]] = doc["worms_id"]

        return self.__taxa_aphia_dict

    def group_contacts_by_role(self, doc: dict, orgs=False):
        """
        Group contacts by role

        :param doc: doc to scan for contacts
        :param orgs: Include organizations
        :return: {"role1": [{doc1}, {doc2}]}
        """

        assert "contacts" in doc.keys()

        people = {p["@people"]: p["role"] for p in doc["contacts"] if "@people" in p.keys()}
        organizations = {o["@organizations"]: o["role"] for o in doc["contacts"] if "@organizations" in o.keys()}

        group = {}
        for doc_id, role in people.items():
            if role not in group.keys():
                group[role] = []
            group[role].append(self.get_document("people", doc_id))

        if orgs:
            for doc_id, role in organizations.items():
                if role not in group.keys():
                    group[role] = []
                group[role].append(self.get_document("organizations", doc_id))

        return group




def get_station_deployments(mc: MetadataCollector, station: dict) -> list:
    return mc.get_station_deployments(station)


def get_sensor_deployments(mc: MetadataCollector, sensor_id: str) -> list:
    return mc.get_sensor_deployments(sensor_id)


def get_sensor_latest_deployment(mc: MetadataCollector, sensor_id: str) -> list:
    """
    Returns the last station where the sensor was deployed
    """
    deployments = get_sensor_deployments(mc, sensor_id)
    return deployments[-1][0]


def get_station_coordinates(mc: MetadataCollector, station: any, timestamp=None) -> (float, float, float):
    return mc.get_station_coordinates(station, timestamp=timestamp)


def get_station_history(mc: MetadataCollector, name: str) -> list:
    return mc.get_station_history(name)




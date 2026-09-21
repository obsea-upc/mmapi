#!/usr/bin/env python3
"""
This script connects to a PostgresQL database and converts from the database to JSON files. Can be used to perform simple
CRUD operations from local files.

author: Enoc Martínez
institution: Universitat Politècnica de Catalunya (UPC)
email: enoc.martinez#upc.edu
license: MIT
created: 21/09/2023
"""
from argparse import ArgumentParser, ArgumentError
from mmm import setup_log
from mmm.common import ask_user_input, dir_list, file_list, assert_type
from mmm.metadata_collector import MetadataCollector, init_metadata_collector, mmapi_collection_names
from datetime import datetime
import yaml
import rich
import os
import time
import json


def load_from_database(mc, subset=[], history=False, verbose=False):
    """
    Loads ALL data from database into dict
    :param mc: MetadataCollector
    :param history: Use history database
    :param verbose: print additional info
    :return: dictionary with all data
    """
    if verbose:
        t = time.time()

    db_data = {}
    if subset:
        collections = subset
    else:
        collections = mc.collection_names
    for collection in collections:
        db_data[collection] = {}
        docs = mc.get_documents(collection, history=history)
        for doc in docs:
            doc_id = doc["#id"]
            db_data[collection][doc_id] = doc
    if verbose:
        rich.print(f"Load all data took {(time.time() - t)*1000:0.1f} msecs")
    return db_data


def process_markdown_to_json(doc: dict, filename: str, collection: str):
    """
    Some documents may have links to Markdown documents to facilitate edits. This process expands the md files into
    strings in the JSON doc.

    { "readme": "&folder/file.md"} <<- this will be expanded

    :param doc:
    :return:
    """
    if collection not in ["datasets"]:
        return doc
    for key, value in doc.items():
        if isinstance(value, str):
            if key.startswith("&") and value.split(".")[-1] == "md":
                directory = os.path.dirname(filename)
                md_file = os.path.join(directory, value)

                if not os.path.isfile(md_file):
                    raise ValueError(f"Cannot find referenced file {md_file}")

                with open(md_file) as f:
                    md_contents = f.read()
                    doc[key] = md_contents

        elif isinstance(value, dict):
            doc[key] = process_markdown_to_json(value, filename, collection)

    return doc

def path_to_collection(path: str):
    col = ""

    if "/" not in path:
        col = path
    else:
        # If we have a path it can be the last or the second-to-last
        splits = path.split("/")
        if splits[-1] in mmapi_collection_names:
            col = splits[-1]
        elif splits[-2] in mmapi_collection_names:
            col = splits[-2]

    if col in mmapi_collection_names:
        return col

    else:
        raise ValueError(f"COuld not find collection in path '{path}'")



def load_doc(filename: str, collection: str):
    """
    Loads a JSON doc. Additionally, loads referenced markdown files
    :param filname:
    :return:
    """
    assert_type(filename, str)
    assert os.path.exists(filename)
    with open(filename) as f:
        try:
            filedata = json.load(f)
        except json.decoder.JSONDecodeError as e:
            rich.print(f"[red]ERROR!! could not load file {filename}, JSON decode error")
            raise e
    collection = path_to_collection(collection)
    return process_markdown_to_json(filedata, filename, collection)



def load_from_filesystem(folder, subset=[], history=False):
    """
    Loads ALL data from database into dict
    :param folder: metadata folder
    :return: dictionary with all data
    """
    fs_data = {}
    assert subset, f"Expected list of collections! got {subset}"

    collections = [os.path.join(folder, collection) for collection in subset]

    for collection in collections:
        collection_name = collection.split("/")[-1]
        files = file_list(collection)
        files = [f for f in files if f.endswith(".json")]
        files = sorted(files)
        fs_data[collection_name] = {}
        for file in files:
            filedata = load_doc(file, collection)
            assert_type(filedata, dict)
            file_id = filedata["#id"]
            if file_id != os.path.basename(file).split(".")[0]:
                rich.print(f"[red]ERROR!! File '{file}' has ID='{file_id}', but it does not match with filename!")
                exit()

            # If we have a subfolder, make it the document group
            last_folder = file.split("/")[-2]
            if last_folder != collection_name:
                # Is the last folder a newly created group?
                group = filedata.get("#group", "")
                if last_folder != group:
                    rich.print(f"assign document '{file_id}' to group '{last_folder}'")
                    filedata["#group"] = last_folder

            if not filedata.get("#group", ""):
                filedata["#group"] = ""

            if history:  # in history we have multiple instances with the same id, so add version at the end
                file_id += f"#v{filedata['#id']}"

            if not history:  # avoid duplicates!
                assert file_id not in fs_data[collection_name].keys(), f"Found duplicated id in '{collection}':'{file_id}'"

            fs_data[collection_name][file_id] = filedata

    return fs_data


def process_markdown_refs(doc, filename, collection,  doc_id=""):
    if not doc_id:
        doc_id = doc["#id"]

    for key, value in doc.items():
        if isinstance(value, str):
            if key.startswith("&"):  # found a markdown reference
                folder = os.path.join(os.path.dirname(filename), "readme")
                os.makedirs(folder, exist_ok=True)
                md_file = os.path.join(folder, f"{doc_id}.md")
                contents = value

                with open(md_file, "w") as f:
                    f.write(contents)

                doc[key] = md_file.split(collection+"/")[-1]

        elif isinstance(value, dict):
            doc[key] = process_markdown_refs(value, filename, collection, doc_id=doc_id)

    return doc


def store_doc_to_filesystem(doc: dict, filename: str, collection: str):
    doc = process_markdown_refs(doc, filename, collection)
    with open(filename, "w") as f:
        f.write(json.dumps(doc, indent=2, ensure_ascii=False))


def store_to_filesystem(data, folder, verbose=False, subset=[], history=False):
    """
    Takes a data dict and stores all objects to the filesystem
    :param folder: folder where to store all the data
    :param verbose: print additional info
    :param data: dict with all the data
    """
    t = time.time()
    n = 0
    for collection in data.keys():
        if subset and collection not in subset:
            continue
        base_path = os.path.join(folder, collection)
        os.makedirs(base_path, exist_ok=True)
        for document_id, document in data[collection].items():

            # If there is a group, process the folder
            group = document["#group"]
            if group:
                folder_path = os.path.join(base_path, group)
                os.makedirs(folder_path, exist_ok=True)
            else:
                folder_path = base_path

            if history:
                version = document["#version"]
                document_filename = os.path.join(folder_path, document_id) + f".v{version}.json"
            else:
                document_filename = os.path.join(folder_path, document_id) + ".json"

            store_doc_to_filesystem(document, document_filename, collection)

            n += 1

    if verbose:
        rich.print(f"{n} written to filesystem took {(time.time() - t)*1000:0.1f} msecs")


def clear_temporal_files(folders: list):
    """
    Removes all temporal files in metadata folder
    :param folder: folder whose content will be erased
    """
    n = 0
    nfolders = 0
    for folder in folders:
        folders = [os.path.join(folder, f) for f in os.listdir(folder)]
        for folder in folders:
            files = [os.path.join(folder, file) for file in os.listdir(folder)]
            for file in files:
                os.remove(file)
                n += 1
            os.rmdir(folder)
            nfolders += 1

    rich.print(f"Deleted {nfolders} folders")
    rich.print(f"Deleted {n} files")


def compare_dicts(doc1: dict, doc2: dict, metadata=False) -> bool:
    """
    Compares two dicts and returns True if there are differences, False if they are equal
    If metadata is False, remove all metadata elements
    :param doc1: dict1
    :param doc2: dict2
    :param metadata: flag that indicates if metadata should also be compared (default False)
    :returns: True if dicts are equal, false if there are differences
    """
    if not metadata:
        # comparing without metadata
        doc1 = MetadataCollector.strip_metadata_fields(doc1)
        doc2 = MetadataCollector.strip_metadata_fields(doc2)
        return doc1 == doc2

    return doc1 == doc2


def compoare_fs_to_db(db_data, fs_data) -> list:
    """
    Compares data in filesystem with data in the database
    :param db_data: data from database
    :param fs_data: data from filesystem
    :return: dict with differences
    """
    diff = []
    for collection in fs_data.keys():
        for doc_id, doc in fs_data[collection].items():
            if doc_id not in db_data[collection].keys():
                rich.print(f"[cyan]{collection} {doc_id} new document!")
                diff.append({"collection": collection, "doc_id": doc_id, "action": "create"})
            elif not compare_dicts(db_data[collection][doc_id], fs_data[collection][doc_id], metadata=True):
                rich.print(f"[green]{collection} {doc_id} modified!")
                diff.append({"collection": collection, "doc_id": doc_id, "action": "replace"})

        for doc_id, doc in db_data[collection].items():
            if doc_id not in fs_data[collection].keys():
                rich.print(f"[red]{collection} {doc_id} deleted")
                diff.append({"collection": collection, "doc_id": doc_id, "action": "delete"})
    return diff


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-s", "--secrets", help="Another argument", type=str, required=False, default="secrets.yaml")
    argparser.add_argument("-g", "--get", help="Get all metadata from a database and stores it to JSON files", action="store_true")
    argparser.add_argument("-p", "--put", help="Puts all metadata from folder and  and stores it to JSON", action="store_true")
    argparser.add_argument("-S", "--status", help="Shows documents that have been modified", action="store_true")
    argparser.add_argument("--clear", help="Clear temporal files", action="store_true")
    argparser.add_argument("-c", "--collections", help="Only use certain collections", nargs="+", default=[])
    argparser.add_argument("-f", "--folder", help="folder to store all metadata, by default 'Metadata'", type=str, default="Metadata")
    argparser.add_argument("-v", "--verbose", help="Show more info", action="store_true")
    argparser.add_argument("--restore", help="Restore a previous doc version (--restore <collection> <doc_id> <version>)", type=str, nargs="+", default=[])
    argparser.add_argument("--clear-history", help="Delete ALL history and reset version to 1", action="store_true")
    argparser.add_argument("--healthcheck", help="Ensure all relations", action="store_true")
    argparser.add_argument("--force", help="Force put, ignore all checks", action="store_true")
    argparser.add_argument("-m", "--force-metadata", help="Force the metadata as is in the document", action="store_true")

    args = argparser.parse_args()
    log = setup_log("metamanager")
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    print("init...")
    mc = init_metadata_collector(secrets, log=log)

    db_name = "metadata"
    db_name_hist = db_name + "_hist"
    folder = os.path.join(args.folder, db_name)
    folder_hist = os.path.join(args.folder, db_name_hist)

    if not os.path.exists(folder_hist):
        os.makedirs(folder_hist)

    collections = mc.collection_names
    if args.collections:
        collections = args.collections

    if args.healthcheck:
        mc.healthcheck(collections=collections)
        exit()

    if args.clear_history:
        if not ask_user_input("[red]WARNING! This will erase ALL history records, do you want to continue"):
            rich.print("cancelling request")
            exit()
        mc.reset_version_history()
        rich.print("Sync filesystem with Database...", end="")
        db_data = load_from_database(mc, subset=collections, verbose=args.verbose)
        store_to_filesystem(db_data, folder, verbose=args.verbose, subset=collections, history=False)
        db_data = load_from_database(mc, subset=collections, verbose=args.verbose, history=True)
        store_to_filesystem(db_data, folder_hist, verbose=args.verbose, subset=collections, history=True)
        rich.print("[green]done!")
        exit()

    force_meta = False
    if args.force_metadata:
        force_meta = True

    os.makedirs(folder, exist_ok=True)
    if args.get and args.put:
        raise ArgumentError("Only get or put arguments can be set!")
    init = time.time()

    if args.restore:
        assert len(args.restore) == 3, "Expected 3 arguments: --restore <collection> <doc_id> <version>"
        collection, doc_id, version = args.restore
        rich.print(f"Restoring: {collection} {doc_id} {version}")
        old_doc = mc.get_document(collection, doc_id, version=int(version))
        rich.print("[orange3]=== The following version will be recovered ===")
        rich.print(old_doc)
        if not ask_user_input("[orange3]continue?"):
            exit()
        new_doc = mc.replace_document(collection, doc_id, old_doc, force=args.force)
        filename = os.path.join(folder, collection, new_doc["#group"], doc_id + ".json")
        rich.print(f"Storing in filesystem: {filename}")
        store_doc_to_filesystem(new_doc, filename, collection)

    if args.clear:
        rich.print("[red]Deleting all files!")
        clear_temporal_files([folder, folder_hist])
        exit()

    if args.get:
        init = time.time()
        rich.print(f"Getting all data from database {mc.db_name} and storing to {folder}...", end="")
        db_data = load_from_database(mc, subset=collections, verbose=args.verbose)
        store_to_filesystem(db_data, folder, verbose=args.verbose, subset=collections, history=False)
        rich.print("[green]ok!")

        rich.print(f"Getting all data from database {mc.db_hist_name    } and storing to {folder_hist}...", end="")
        db_data = load_from_database(mc, subset=collections, verbose=args.verbose, history=True)
        store_to_filesystem(db_data, folder_hist, verbose=args.verbose, subset=collections, history=True)
        rich.print("[green]ok!")
        rich.print(f"[green]done! took {1000*(time.time() - init):.03} msecs")
        exit()

    diff = []  # list of elements to be updated
    if args.put or args.status:
        db_data = load_from_database(mc, subset=collections)
        fs_data = load_from_filesystem(folder, subset=collections)
        diff = compoare_fs_to_db(db_data, fs_data)

        # checking also historical data
        db_data_hist = load_from_database(mc, subset=collections, history=True)
        fs_data_hist = load_from_filesystem(folder_hist, subset=collections, history=True)

    if args.put:
        replaced = 0
        inserted = 0
        deleted = 0

        diff = compoare_fs_to_db(db_data, fs_data)

        for record in diff:
            collection = record["collection"]
            document_id = record["doc_id"]
            action = record["action"]
            if collection not in collections:
                continue
            rich.print(f"action: {action}")
            if action == "replace":

                # replacing a document in the DB. To prevent re-uploading old documents, check that the filesystem
                # modification time is greater than the actual DB modification time
                fs_doc = fs_data[collection][document_id]
                db_doc = db_data[collection][document_id]
                fs_mtime = os.path.getmtime(os.path.join(folder, collection, fs_doc["#group"], fs_doc["#id"] + ".json"))
                db_mtime = datetime.fromisoformat(db_doc["#modificationDate"]).timestamp()
                if not args.force and  db_mtime > fs_mtime:
                    rich.print(f"[yellow]Ignoring {collection}:{document_id} doc in filesystem is older than in DB! use --force to overwrite")
                    continue

                mc.replace_document(collection, document_id,  fs_data[collection][document_id], force=args.force)
                mc.get_document(collection, document_id)  # update the local document
                replaced += 1
            elif action == "create":
                mc.insert_document(collection, fs_data[collection][document_id], force=args.force)
                inserted += 1

            elif action == "delete":
                if ask_user_input(f"[red]Do you want to delete document '{document_id}'?"):
                    mc.delete_document(collection, document_id)
                    rich.print("document deleted")
                    deleted += 1
                else:
                    rich.print("document kept in DB")
            else:
                raise ValueError("Unknwon operation")

        rich.print(f"MetadataManager summary:")
        rich.print(f"    Documents inserted: {inserted}")
        rich.print(f"    Documents updated:  {replaced}")
        rich.print(f"    Documents deleted:  {deleted}")

    rich.print(f"[cyan]All tasks finished, took {1000*(time.time() - init):.03f} msecs")

#!/usr/bin/env python3
"""
zenodo_manager.py

Comprehensive CLI for managing Zenodo drafts, published records, and
community inclusion requests.

Usage:
    python3 zenodo_manager.py <element> <action> [options]

Elements:
    draft       Unpublished draft records owned by you
    record      Published records owned by you
    request     Requests involving you (optionally filtered by community)

Actions:
    list        List matching items
    delete      Delete matching items (draft: discard, record: delete, request: cancel)
    accept      Accept matching items (request only)
    reject      Decline matching items (request only)

Examples:
    python3 zenodo_manager.py draft list
    python3 zenodo_manager.py draft delete
    python3 zenodo_manager.py record delete
    python3 zenodo_manager.py record delete -f 571569 571598
    python3 zenodo_manager.py request list
    python3 zenodo_manager.py request list -c my-community
    python3 zenodo_manager.py request accept -c my-community
    python3 zenodo_manager.py request reject -c my-community another-community
"""

import argparse
import logging
import sys
import time

import requests
import yaml
from rich.console import Console
from rich.logging import RichHandler
from rich.text import Text

ELEMENTS = ("draft", "record", "request")
ACTIONS = ("list", "delete", "accept", "reject")
REQUEST_ONLY_ACTIONS = ("accept", "reject")

# Transient server-side errors worth retrying before giving up on an item
RETRYABLE_STATUS_CODES = {502, 503, 504}


# ---------------------------------------------------------------------- #
# Logging
# ---------------------------------------------------------------------- #
class PlainFileFormatter(logging.Formatter):
    """Strips rich markup tags (e.g. '[bold red]...[/bold red]') before writing to file."""
    def format(self, record):
        formatted = super().format(record)
        return Text.from_markup(formatted).plain


def setup_logging(log_level: str, log_file: str = "zenodo_manager.log") -> logging.Logger:
    logger = logging.getLogger("zenodo_manager")
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()

    console = Console()
    rich_handler = RichHandler(
        console=console,
        markup=True,
        rich_tracebacks=True,
        show_path=False,
    )
    rich_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(rich_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        PlainFileFormatter("%(asctime)s %(levelname)-7s: [%(name)s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")
    )
    logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------- #
# Zenodo manager
# ---------------------------------------------------------------------- #
class ZenodoManager:
    """
    Thin client around the Zenodo/InvenioRDM REST API for managing
    drafts, published records, and community/other requests.

    URL and token are configured once at construction time.
    """

    def __init__(self, url: str, token: str, log: logging.Logger = None):
        self.url = url.rstrip("/")
        self.token = token
        self.log = log or logging.getLogger("zenodo_manager")

    # ------------------------------------------------------------------ #
    # Low-level helpers
    # ------------------------------------------------------------------ #
    def _headers(self, json_headers: bool = False) -> dict:
        headers = {"Authorization": f"Bearer {self.token}"}
        if json_headers:
            headers["Content-Type"] = "application/json"
        return headers

    def _http_response(self, r: requests.Response) -> None:
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            self.log.error(f"[bold red]{r.status_code} error[/bold red] for {r.url}")
            self.log.error(f"Response: {r.text}")
            raise e

    def _paginate(self, url: str, params: dict = None) -> list[dict]:
        """Follows Zenodo/InvenioRDM's hits.hits / links.next pagination pattern."""
        all_hits = []
        next_url = url
        next_params = params

        while next_url:
            r = requests.get(next_url, params=next_params, headers=self._headers(), timeout=60)
            self._http_response(r)
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            all_hits.extend(hits)

            next_url = data.get("links", {}).get("next")
            next_params = None  # 'next' link is already fully formed

        return all_hits

    @staticmethod
    def _is_never_published(hit: dict) -> bool:
        """
        True if the hit is a draft that has never been published.

        Zenodo's /user/records returns the legacy deposit serialization,
        which has no 'is_draft'/'is_published' fields (and ignores the
        'is_published' query param). Instead it exposes:
          - status: 'draft' | 'published'
          - state: 'unsubmitted' (new draft), 'inprogress' (published
            record with an open edit), 'done' (published)
          - submitted: False only for records that were never published
        Plain InvenioRDM fields are honoured if present.
        """
        if "is_published" in hit and hit["is_published"] is not None:
            return not hit["is_published"]
        if "submitted" in hit:
            return not hit["submitted"]
        return hit.get("state") == "unsubmitted" or bool(hit.get("is_draft", False))

    @staticmethod
    def _filter_by_ids(items: list[dict], filter_ids: list[str], id_extractor) -> list[dict]:
        """Keeps only items whose extracted id is in filter_ids (as strings)."""
        if not filter_ids:
            return items
        wanted = {str(f) for f in filter_ids}
        return [item for item in items if str(id_extractor(item)) in wanted]

    def _call_with_retry(self, method: str, url: str, max_retries: int = 2,
                          backoff_seconds: float = 3.0, **kwargs) -> requests.Response:
        """
        Performs an HTTP call, retrying a limited number of times on
        transient gateway errors (502/503/504) or connection issues,
        before giving up and re-raising.
        """
        attempt = 0
        while True:
            attempt += 1
            try:
                r = requests.request(method, url, headers=self._headers(), timeout=60, **kwargs)
                if r.status_code in RETRYABLE_STATUS_CODES and attempt <= max_retries:
                    self.log.warning(
                        f"[yellow]{r.status_code}[/yellow] from {url} "
                        f"(attempt {attempt}/{max_retries + 1}); retrying in {backoff_seconds:.0f}s..."
                    )
                    time.sleep(backoff_seconds)
                    continue
                self._http_response(r)
                return r
            except requests.exceptions.ConnectionError as e:
                if attempt <= max_retries:
                    self.log.warning(
                        f"[yellow]Connection error[/yellow] for {url} "
                        f"(attempt {attempt}/{max_retries + 1}); retrying in {backoff_seconds:.0f}s..."
                    )
                    time.sleep(backoff_seconds)
                    continue
                raise

    # ------------------------------------------------------------------ #
    # Drafts
    # ------------------------------------------------------------------ #
    def list_drafts(self, filter_ids: list[str] = None) -> list[dict]:
        """
        Lists unpublished draft records owned by the current user.

        Published records with a pending edit (state 'inprogress') are
        not included, since they are still published records.
        """
        url = f"{self.url}/user/records"
        params = {"size": 50, "page": 1}
        candidates = self._paginate(url, params)

        drafts = [d for d in candidates if self._is_never_published(d)]

        for d in candidates:
            if not self._is_never_published(d) and d.get("state") == "inprogress":
                self.log.debug(
                    f"Skipping published record {d.get('id')} with an unpublished edit in progress."
                )
        if len(drafts) != len(candidates):
            self.log.debug(
                f"Filtered out {len(candidates) - len(drafts)} published item(s) "
                f"returned by the server."
            )

        drafts = self._filter_by_ids(drafts, filter_ids, lambda d: d.get("id"))
        self.log.info(f"Found [bold cyan]{len(drafts)}[/bold cyan] draft(s).")
        return drafts

    def delete_draft(self, record_id: str) -> None:
        """Discards a single draft record."""
        url = f"{self.url}/records/{record_id}/draft"
        r = requests.delete(url, headers=self._headers(), timeout=60)
        self._http_response(r)
        self.log.info(f"Deleted draft [bold]{record_id}[/bold]")

    def delete_all_drafts(self, filter_ids: list[str] = None) -> None:
        drafts = self.list_drafts(filter_ids)
        if not drafts:
            self.log.info("No drafts to delete.")
            return
        self.log.warning(f"About to delete [bold yellow]{len(drafts)}[/bold yellow] draft(s).")
        for d in drafts:
            record_id = d.get("id")
            title = d.get("metadata", {}).get("title", "<no title>")
            self.log.info(f"Deleting draft [bold]{record_id}[/bold] (\"{title}\")")
            try:
                self.delete_draft(record_id)
            except requests.exceptions.RequestException:
                self.log.error(f"[bold red]Failed[/bold red] to delete draft {record_id}")

    # ------------------------------------------------------------------ #
    # Records
    # ------------------------------------------------------------------ #
    def list_records(self, filter_ids: list[str] = None) -> list[dict]:
        """
        Lists published records owned by the current user.

        Includes published records that have an edit in progress.
        """
        url = f"{self.url}/user/records"
        params = {"size": 50, "page": 1}
        candidates = self._paginate(url, params)

        records = [r for r in candidates if not self._is_never_published(r)]

        records = self._filter_by_ids(records, filter_ids, lambda r: r.get("id"))
        self.log.info(f"Found [bold cyan]{len(records)}[/bold cyan] published record(s).")
        return records

    def delete_record(self, record_id: str) -> None:
        """
        Deletes a published record.

        Note: Zenodo/InvenioRDM generally restricts deleting *published*
        records to instance administrators — a regular user token will
        likely get a 403 here. That's expected, not a bug in this script.
        """
        url = f"{self.url}/records/{record_id}"
        r = requests.delete(url, headers=self._headers(), timeout=60)
        self._http_response(r)
        self.log.info(f"Deleted record [bold]{record_id}[/bold]")

    def delete_all_records(self, filter_ids: list[str] = None) -> None:
        records = self.list_records(filter_ids)
        if not records:
            self.log.info("No records to delete.")
            return
        self.log.warning(f"About to delete [bold yellow]{len(records)}[/bold yellow] published record(s).")
        for rec in records:
            record_id = rec.get("id")
            title = rec.get("metadata", {}).get("title", "<no title>")
            self.log.info(f"Deleting record [bold]{record_id}[/bold] (\"{title}\")")
            try:
                self.delete_record(record_id)
            except requests.exceptions.RequestException:
                self.log.error(
                    f"[bold red]Failed[/bold red] to delete record {record_id} "
                    f"(usually requires admin rights on Zenodo)."
                )

    # ------------------------------------------------------------------ #
    # Requests
    # ------------------------------------------------------------------ #
    def list_requests(self, communities: list[str] = None, status: str = "submitted",
                       filter_ids: list[str] = None) -> list[dict]:
        """
        Lists requests involving the current user (as submitter or receiver),
        matching Zenodo's "My requests" dashboard view.

        Community filtering is applied client-side rather than via a
        server-side query field, since community-scoped queries were
        returning zero results even when matching requests existed.
        """
        url = f"{self.url}/requests"
        params = {"size": 50, "page": 1}
        if status:
            params["q"] = f"status:{status}"

        hits = self._paginate(url, params)
        self.log.info(f"Fetched [bold cyan]{len(hits)}[/bold cyan] request(s) with status '{status}'.")

        if communities:
            hits = [
                req for req in hits
                if req.get("receiver", {}).get("community") in communities
            ]
            self.log.info(
                f"Filtered to [bold cyan]{len(hits)}[/bold cyan] request(s) "
                f"for communities {communities}."
            )

        def _req_id_and_record(req: dict):
            return req.get("id"), req.get("topic", {}).get("record")

        if filter_ids:
            wanted = {str(f) for f in filter_ids}
            hits = [
                req for req in hits
                if str(_req_id_and_record(req)[0]) in wanted
                or str(_req_id_and_record(req)[1]) in wanted
            ]
            self.log.info(f"Filtered to [bold cyan]{len(hits)}[/bold cyan] request(s) by -f/--filter.")

        return hits

    def _apply_request_action(self, request_obj: dict, action: str) -> dict:
        action_url = request_obj.get("links", {}).get("actions", {}).get(action)
        if not action_url:
            request_id = request_obj["id"]
            action_url = f"{self.url}/requests/{request_id}/actions/{action}"

        r = self._call_with_retry("POST", action_url)
        return r.json()

    def accept_requests(self, communities: list[str] = None, filter_ids: list[str] = None) -> None:
        pending = self.list_requests(communities, status="submitted", filter_ids=filter_ids)
        if not pending:
            self.log.info("No pending requests to accept.")
            return
        for req in pending:
            req_id, title = req.get("id"), req.get("title", "<no title>")
            self.log.info(f"[bold green]Accepting[/bold green] request {req_id} (\"{title}\")")
            try:
                self._apply_request_action(req, "accept")
            except requests.exceptions.RequestException:
                self.log.error(
                    f"[bold red]Failed[/bold red] to accept request {req_id} "
                    f"(record behind this request may have been updated or deleted); skipping."
                )

    def reject_requests(self, communities: list[str] = None, filter_ids: list[str] = None) -> None:
        pending = self.list_requests(communities, status="submitted", filter_ids=filter_ids)
        if not pending:
            self.log.info("No pending requests to reject.")
            return
        for req in pending:
            req_id, title = req.get("id"), req.get("title", "<no title>")
            self.log.info(f"[bold yellow]Declining[/bold yellow] request {req_id} (\"{title}\")")
            try:
                self._apply_request_action(req, "decline")
            except requests.exceptions.RequestException:
                self.log.error(f"[bold red]Failed[/bold red] to decline request {req_id}; skipping.")

    def delete_requests(self, communities: list[str] = None, filter_ids: list[str] = None) -> None:
        """
        'Deleting' a request maps to cancelling it — the only destructive
        action available on a request (and only to its submitter).
        """
        pending = self.list_requests(communities, status="submitted", filter_ids=filter_ids)
        if not pending:
            self.log.info("No pending requests to delete/cancel.")
            return
        for req in pending:
            req_id, title = req.get("id"), req.get("title", "<no title>")
            self.log.info(f"Cancelling request [bold]{req_id}[/bold] (\"{title}\")")
            try:
                self._apply_request_action(req, "cancel")
            except requests.exceptions.RequestException:
                self.log.error(f"[bold red]Failed[/bold red] to cancel request {req_id}; skipping.")


# ---------------------------------------------------------------------- #
# CLI
# ---------------------------------------------------------------------- #
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Comprehensive Zenodo management CLI (drafts, records, requests).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("element", choices=ELEMENTS, help="Element to manage: draft, record, or request")
    parser.add_argument("action", choices=ACTIONS, help="Action to perform: list, delete, accept, or reject")

    parser.add_argument("-c", "--communities", nargs="+", default=[],
                         help="Optional list of community ids to filter requests by (request element only)")
    parser.add_argument("-f", "--filter", nargs="+", default=[], dest="filter_ids",
                         help="Optional list of ids to restrict the action to "
                              "(record/draft id for draft/record; request id or record id for request)")
    parser.add_argument("-s", "--secrets", default="secrets.yaml",
                         help="Path to secrets file (default: secrets.yaml)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Shortcut for --log-level debug")
    parser.add_argument("-l", "--log-level", default="info",
                         choices=["debug", "info", "warning", "error"],
                         help="Logging level (default: info)")

    return parser


def main():
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.action in REQUEST_ONLY_ACTIONS and args.element != "request":
        parser.error(f"Action '{args.action}' is only valid for the 'request' element.")

    log_level = "debug" if args.verbose else args.log_level
    logger = setup_logging(log_level)

    logger.info(f"Loading secrets from [bold]{args.secrets}[/bold]")
    with open(args.secrets) as f:
        secrets = yaml.safe_load(f)["secrets"]
    manager = ZenodoManager(url=secrets["zenodo"]["url"], token=secrets["zenodo"]["token"], log=logger)

    logger.info(f"Running: [bold]{args.element} {args.action}[/bold]")
    if args.filter_ids:
        logger.info(f"Filter active: [bold]{args.filter_ids}[/bold]")

    if args.element == "draft":
        if args.action == "list":
            for d in manager.list_drafts(args.filter_ids):
                title = d.get("metadata", {}).get("title", "<no title>")
                logger.info(f"  draft [bold]{d.get('id')}[/bold] | \"{title}\"")
        elif args.action == "delete":
            manager.delete_all_drafts(args.filter_ids)

    elif args.element == "record":
        if args.action == "list":
            for rec in manager.list_records(args.filter_ids):
                title = rec.get("metadata", {}).get("title", "<no title>")
                logger.info(f"  record [bold]{rec.get('id')}[/bold] | \"{title}\"")
        elif args.action == "delete":
            manager.delete_all_records(args.filter_ids)

    elif args.element == "request":
        if args.action == "list":
            for req in manager.list_requests(args.communities, filter_ids=args.filter_ids):
                title = req.get("title", "<no title>")
                logger.info(f"  request [bold]{req.get('id')}[/bold] | \"{title}\"")
        elif args.action == "delete":
            manager.delete_requests(args.communities, args.filter_ids)
        elif args.action == "accept":
            manager.accept_requests(args.communities, args.filter_ids)
        elif args.action == "reject":
            manager.reject_requests(args.communities, args.filter_ids)

    logger.info("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
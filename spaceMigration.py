#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import time
import json
import math
import typing as t
from urllib.parse import urljoin, quote
import requests

# =========================
# ===== CONFIGURATION =====
# =========================
# --- SOURCE (read from) ---
SRC_BASE_URL   = "<source_confluence_space_url>"     # include '/wiki'
SRC_SPACE_KEY  = "<source_confluence_space_key>"
SRC_USERNAME   = "<your_email>"
SRC_API_TOKEN  = "<your_PAT_token>"

# --- DESTINATION (write to) ---
DST_BASE_URL   = "<destination_confluence_space_url>"       # include '/wiki'
DST_SPACE_KEY  = "<destination_confluence_space_key>"
DST_USERNAME   = "<your_email>"
DST_API_TOKEN  = "<your_PAT_token>"

# --- What to copy ---
COPY_ATTACHMENTS = True
COPY_LABELS      = True

# --- Behavior ---
ON_TITLE_CONFLICT = "update"     # "skip" | "update" | "append-suffix"
PAGE_LIMIT        = 200          # pagination page size (<=200 typical)
RETRY_MAX         = 6            # retries on 429/5xx
RETRY_BASE_WAIT   = 2.0          # seconds (exponential backoff base)
TIMEOUT_S         = 60           # per-request timeout

# =========================
# ====== HTTP HELPERS =====
# =========================
def _session(user: str, token: str) -> requests.Session:
    s = requests.Session()
    s.auth = (user, token)
    s.headers.update({"Accept": "application/json"})
    return s

def _req_with_retry(sess: requests.Session, method: str, url: str, **kw) -> requests.Response:
    # Basic 429/5xx retry with exponential backoff and Retry-After support.
    for attempt in range(1, RETRY_MAX + 1):
        try:
            r = sess.request(method, url, timeout=TIMEOUT_S, **kw)
        except requests.RequestException as e:
            if attempt >= RETRY_MAX:
                raise
            wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
            time.sleep(wait)
            continue

        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            wait = float(ra) if ra else RETRY_BASE_WAIT * (2 ** (attempt - 1))
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600:
            if attempt >= RETRY_MAX:
                return r
            wait = RETRY_BASE_WAIT * (2 ** (attempt - 1))
            time.sleep(wait)
            continue

        return r
    # should not reach here
    return r

def _paged_get(sess: requests.Session, url: str) -> t.Iterator[dict]:
    """
    Iterate v1 paginated responses using start/limit and _links.next
    """
    next_url = url
    while next_url:
        r = _req_with_retry(sess, "GET", next_url)
        r.raise_for_status()
        data = r.json()
        for item in data.get("results", []):
            yield item
        # _links.next is relative path
        next_rel = data.get("_links", {}).get("next")
        if next_rel:
            base = data.get("_links", {}).get("base", "")
            # Prefer absolute: base + next
            next_url = base.rstrip("/") + next_rel
        else:
            next_url = None

# =========================
# ====== CORE LOGIC =======
# =========================
def fetch_all_pages_from_space(src_sess: requests.Session) -> list[dict]:
    """
    Pull all pages from the source space with storage & ancestors expanded.
    """
    q = (
        f"{SRC_BASE_URL.rstrip('/')}/rest/api/content"
        f"?type=page&spaceKey={quote(SRC_SPACE_KEY)}"
        f"&expand=body.storage,ancestors,version"
        f"&limit={PAGE_LIMIT}"
    )
    pages = list(_paged_get(src_sess, q))
    return pages

def sort_pages_parent_first(pages: list[dict]) -> list[dict]:
    # Use number of ancestors as depth; parents (smaller depth) first
    def depth(p: dict) -> int:
        return len(p.get("ancestors", []) or [])
    return sorted(pages, key=depth)

def find_dest_page_by_title(
    dst_sess: requests.Session, title: str, parent_id: str | None
) -> dict | None:
    """
    Try to locate an existing destination page with the same title under the same parent.
    """
    # First find candidates by title in the space
    url = (
        f"{DST_BASE_URL.rstrip('/')}/rest/api/content"
        f"?type=page&spaceKey={quote(DST_SPACE_KEY)}&title={quote(title)}"
        f"&expand=ancestors"
    )
    r = _req_with_retry(dst_sess, "GET", url)
    if r.status_code != 200:
        return None
    data = r.json()
    for p in data.get("results", []):
        ancs = p.get("ancestors", []) or []
        direct_parent = ancs[-1]["id"] if ancs else None
        if (parent_id or None) == (direct_parent or None):
            return p
    return None

def create_page(
    dst_sess: requests.Session,
    title: str,
    storage_value: str,
    parent_id: str | None
) -> dict:
    body = {
        "type": "page",
        "title": title,
        "space": {"key": DST_SPACE_KEY},
        "body": {
            "storage": {
                "value": storage_value,
                "representation": "storage"
            }
        }
    }
    if parent_id:
        body["ancestors"] = [{"id": parent_id}]

    url = f"{DST_BASE_URL.rstrip('/')}/rest/api/content"
    r = _req_with_retry(dst_sess, "POST", url, json=body,
                        headers={"Content-Type": "application/json"})
    r.raise_for_status()
    return r.json()

def update_page(
    dst_sess: requests.Session, page: dict, new_storage_value: str
) -> dict:
    page_id = page["id"]
    version_num = page.get("version", {}).get("number", 1)
    body = {
        "id": page_id,
        "type": "page",
        "title": page["title"],
        "space": {"key": DST_SPACE_KEY},
        "body": {
            "storage": {
                "value": new_storage_value,
                "representation": "storage"
            }
        },
        "version": {"number": version_num + 1}
    }
    url = f"{DST_BASE_URL.rstrip('/')}/rest/api/content/{page_id}"
    r = _req_with_retry(dst_sess, "PUT", url, json=body,
                        headers={"Content-Type": "application/json"})
    r.raise_for_status()
    return r.json()

def copy_labels(src_sess: requests.Session, dst_sess: requests.Session, src_id: str, dst_id: str):
    if not COPY_LABELS:
        return
    # GET labels from source
    get_url = f"{SRC_BASE_URL.rstrip('/')}/rest/api/content/{src_id}/label"
    r = _req_with_retry(src_sess, "GET", get_url)
    if r.status_code != 200:
        return
    labels = r.json().get("results", [])
    if not labels:
        return
    payload = [{"prefix": l.get("prefix", "global"), "name": l["name"]} for l in labels if "name" in l]
    # POST labels to dest
    post_url = f"{DST_BASE_URL.rstrip('/')}/rest/api/content/{dst_id}/label"
    r = _req_with_retry(dst_sess, "POST", post_url, json=payload,
                        headers={"Content-Type": "application/json"})
    # ignore non-200 silently

def copy_attachments(src_sess: requests.Session, dst_sess: requests.Session, src_id: str, dst_id: str):
    if not COPY_ATTACHMENTS:
        return
    # List attachments on source
    list_url = (
        f"{SRC_BASE_URL.rstrip('/')}/rest/api/content/{src_id}/child/attachment"
        f"?limit={PAGE_LIMIT}"
    )
    for att in _paged_get(src_sess, list_url):
        # Download
        dl_rel = att.get("_links", {}).get("download")
        filename = att.get("title", "attachment.bin")
        if not dl_rel:
            continue
        dl_url = SRC_BASE_URL.rstrip("/") + dl_rel
        dl = _req_with_retry(src_sess, "GET", dl_url, stream=True)
        if dl.status_code != 200:
            continue
        content = io.BytesIO(dl.content)

        # Upload to destination
        up_url = f"{DST_BASE_URL.rstrip('/')}/rest/api/content/{dst_id}/child/attachment"
        headers = {"X-Atlassian-Token": "nocheck"}  # required for multipart
        files = {"file": (filename, content, "application/octet-stream")}
        r = _req_with_retry(dst_sess, "POST", up_url, files=files, headers=headers)
        # ignore non-200 silently

def run_copy():
    src = _session(SRC_USERNAME, SRC_API_TOKEN)
    dst = _session(DST_USERNAME, DST_API_TOKEN)

    print(f"Reading pages from space '{SRC_SPACE_KEY}' at {SRC_BASE_URL} ...")
    pages = fetch_all_pages_from_space(src)
    if not pages:
        print("No pages found. Nothing to do.")
        return

    # sort by depth so parents are created before children
    pages = sort_pages_parent_first(pages)

    # Map source page id -> destination page id
    id_map: dict[str, str] = {}

    # Optionally, detect an existing space home page in destination to use as logical root
    # (not required; we rely on ancestors mapping).
    for p in pages:
        src_id    = p["id"]
        title     = p["title"]
        storage   = p.get("body", {}).get("storage", {}).get("value", "") or ""
        ancestors = p.get("ancestors", []) or []
        parent_src_id = ancestors[-1]["id"] if ancestors else None
        parent_dst_id = id_map.get(parent_src_id) if parent_src_id else None

        existing = find_dest_page_by_title(dst, title, parent_dst_id)
        if existing:
            if ON_TITLE_CONFLICT == "skip":
                print(f"SKIP (exists)  {title}")
                id_map[src_id] = existing["id"]
                continue
            elif ON_TITLE_CONFLICT == "update":
                print(f"UPDATE         {title}")
                updated = update_page(dst, existing, storage)
                id_map[src_id] = updated["id"]
            else:  # append-suffix
                title = f"{title} (copy)"
                print(f"CREATE(+suffix){title}")
                created = create_page(dst, title, storage, parent_dst_id)
                id_map[src_id] = created["id"]
        else:
            print(f"CREATE         {title}")
            created = create_page(dst, title, storage, parent_dst_id)
            id_map[src_id] = created["id"]

        # Copy labels & attachments
        dst_id = id_map[src_id]
        if COPY_LABELS:
            copy_labels(src, dst, src_id, dst_id)
        if COPY_ATTACHMENTS:
            copy_attachments(src, dst, src_id, dst_id)

    print("✅ Done. Copied pages:", len(id_map))
    print(f"Destination space: {DST_SPACE_KEY} at {DST_BASE_URL}")

if __name__ == "__main__":
    run_copy()

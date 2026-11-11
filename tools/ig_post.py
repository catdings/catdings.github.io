"""Post due Instagram Reels from queue.json (run by .github/workflows/instagram.yml every 10 minutes).

queue.json: [{"slug", "at" (ISO with timezone), "video_url", "caption", "thumb_offset_ms", "status": "queued" |
"posted" | "failed", "media_id", "permalink", "posted_at", "error", "tries"}]
Secrets (GitHub Actions): IG_TOKEN (non-expiring Page token), IG_USER_ID. Never printed.
Standard library only, so the runner needs no installs.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

GRAPH = "https://graph.facebook.com/v27.0"
QUEUE = Path(__file__).resolve().parent.parent / "queue.json"
MAX_TRIES = 6


def call(method, url, **params):
    data = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url + ("" if method == "POST" else "?" + data.decode()),
                                 data=data if method == "POST" else None, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        raise RuntimeError(f"{e.code} {json.loads(body).get('error', {}).get('message', body)[:200] if body.startswith('{') else body[:200]}")


def post(item, token, ig):
    c = call("POST", f"{GRAPH}/{ig}/media", media_type="REELS", video_url=item["video_url"], caption=item["caption"][:2200],
             share_to_feed="true", thumb_offset=str(item.get("thumb_offset_ms", 0)), access_token=token)
    for _ in range(40):  # Meta fetches and processes the video; poll up to ~10 min
        s = call("GET", f"{GRAPH}/{c['id']}", fields="status_code,status", access_token=token)
        if s.get("status_code") == "FINISHED":
            break
        if s.get("status_code") in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"processing failed: {s}")
        time.sleep(15)
    else:
        raise RuntimeError("still processing after 10 min")
    media = call("POST", f"{GRAPH}/{ig}/media_publish", creation_id=c["id"], access_token=token)["id"]
    link = call("GET", f"{GRAPH}/{media}", fields="permalink", access_token=token).get("permalink", "")
    return media, link


def main():
    token, ig = os.environ.get("IG_TOKEN"), os.environ.get("IG_USER_ID")
    if not token or not ig:
        sys.exit("IG_TOKEN / IG_USER_ID secrets are missing")
    queue = json.loads(QUEUE.read_text(encoding="utf-8")) if QUEUE.exists() else []
    now = datetime.now(timezone.utc)
    changed = False
    for item in queue:
        if item.get("status") != "queued" or datetime.fromisoformat(item["at"]) > now:
            continue
        item["tries"] = item.get("tries", 0) + 1
        try:
            item["media_id"], item["permalink"] = post(item, token, ig)
            item["status"], item["posted_at"] = "posted", now.isoformat(timespec="minutes")
            item.pop("error", None)
            print(f"posted {item['slug']}: {item['permalink']}")
        except (RuntimeError, urllib.error.URLError, KeyError) as e:
            item["error"] = str(e)[:300]
            if item["tries"] >= MAX_TRIES:
                item["status"] = "failed"
            print(f"{item['slug']}: try {item['tries']} failed: {item['error']}")
        changed = True
    if changed:
        QUEUE.write_text(json.dumps(queue, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print("changed" if changed else "nothing due")


if __name__ == "__main__":
    main()

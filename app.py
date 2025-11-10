import os
import uuid
import threading
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify, send_from_directory, url_for
from scraper import scrape_osm_keyword, scrape_website_email

app = Flask(__name__, template_folder="templates", static_folder="static")

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT_DIR, exist_ok=True)

# In-memory jobs store
# jobs[job_id] = {
#   "status": "running|finished|failed",
#   "keywords": {kw: {count:int, status:str, last_msg:str}},
#   "total": N,
#   "completed": M,
#   "file": path,
#   "error": optional
# }
jobs = {}
EXECUTOR_WORKERS = 5  # number of concurrent keyword tasks
OSM_DELAY_BETWEEN_REQUESTS = 1.0  # seconds between Overpass calls per thread (politeness)
WEBSITE_EMAIL_TIMEOUT = 6  # seconds per website fetch

# Helper: dedupe results by name+lat+lng
def dedupe_rows(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r.get("name","").strip().lower(), str(r.get("lat")), str(r.get("lng")))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def run_job(job_id, keywords, city, max_per_keyword, try_website_emails):
    jobs[job_id]["status"] = "running"
    jobs[job_id]["total"] = len(keywords)
    jobs[job_id]["completed"] = 0
    jobs[job_id]["keywords"] = {k: {"count": 0, "status": "queued", "last_msg": ""} for k in keywords}
    combined_results = []

    # limit number of worker threads to avoid Overpass overload
    with ThreadPoolExecutor(max_workers=EXECUTOR_WORKERS) as executor:
        future_to_kw = {}
        for kw in keywords:
            # submit worker for each keyword
            future = executor.submit(worker_for_keyword, job_id, kw, city, max_per_keyword, try_website_emails)
            future_to_kw[future] = kw

        for future in as_completed(future_to_kw):
            kw = future_to_kw[future]
            try:
                res_rows = future.result()
                # append rows
                combined_results.extend(res_rows)
                jobs[job_id]["keywords"][kw]["status"] = "done"
            except Exception as e:
                jobs[job_id]["keywords"][kw]["status"] = "failed"
                jobs[job_id]["keywords"][kw]["last_msg"] = str(e)

            # update completed count
            jobs[job_id]["completed"] += 1

    # dedupe and write CSV
    combined_results = dedupe_rows(combined_results)
    out_file = os.path.join(OUT_DIR, f"{job_id}.csv")
    keys = ["name", "address", "category", "phone", "email", "website", "lat", "lng", "keyword"]
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in combined_results:
            writer.writerow({k: r.get(k,"") for k in keys})

    jobs[job_id]["file"] = out_file
    jobs[job_id]["status"] = "finished"
    return

def worker_for_keyword(job_id, keyword, city, max_per_keyword, try_website_emails):
    # update status
    jobs[job_id]["keywords"][keyword]["status"] = "running"
    jobs[job_id]["keywords"][keyword]["last_msg"] = "Querying Overpass..."
    # call scraper function (which includes polite sleep)
    rows = scrape_osm_keyword(keyword, city, max_per_keyword, delay=OSM_DELAY_BETWEEN_REQUESTS, progress_cb=lambda c: None)
    # rows: list of dicts with fields (name,address,category,phone,website,lat,lng,email)
    # If try_website_emails is True, try to fetch email from website when email missing
    if try_website_emails:
        for i, r in enumerate(rows):
            if not r.get("email") and r.get("website"):
                try:
                    r["email"] = scrape_website_email(r["website"], timeout=WEBSITE_EMAIL_TIMEOUT)
                except Exception:
                    pass
                # small polite delay
                time.sleep(0.2)
    # add keyword field to each result
    for r in rows:
        r["keyword"] = keyword
    # update job meta
    jobs[job_id]["keywords"][keyword]["count"] = len(rows)
    jobs[job_id]["keywords"][keyword]["last_msg"] = f"Found {len(rows)}"
    return rows

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/start", methods=["POST"])
def start():
    data = request.form or request.get_json() or {}
    # get keywords from textarea or file upload
    keywords_text = data.get("keywords", "") or ""
    uploaded = None
    if 'file' in request.files:
        uploaded = request.files['file']
    if uploaded and uploaded.filename:
        content = uploaded.read().decode('utf-8', errors='ignore')
        keywords_text = keywords_text + "\n" + content

    # normalize keywords: one per line, strip empties
    keywords = []
    for line in keywords_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # allow comma-separated in a line
        parts = [p.strip() for p in line.split(",") if p.strip()]
        keywords.extend(parts)

    # dedupe keywords while preserving order
    seen = set()
    uniq_keywords = []
    for k in keywords:
        kl = k.lower()
        if kl in seen:
            continue
        seen.add(kl)
        uniq_keywords.append(k)

    # limit to a reasonable number (you asked >=100; allow up to 500 but warn)
    max_keywords_allowed = 500
    if len(uniq_keywords) > max_keywords_allowed:
        uniq_keywords = uniq_keywords[:max_keywords_allowed]

    city = data.get("city", "").strip()
    try:
        max_per_keyword = int(data.get("max_per_keyword", 50))
    except:
        max_per_keyword = 50
    try_website_emails = bool(data.get("try_emails", "") in ("true","True","1","on"))

    if not uniq_keywords:
        return jsonify({"error":"no keywords provided"}), 400
    if not city:
        return jsonify({"error":"city required"}), 400

    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status":"queued", "keywords":{}, "total":len(uniq_keywords), "completed":0, "file":None}

    # start background thread
    thread = threading.Thread(target=run_job, args=(job_id, uniq_keywords, city, max_per_keyword, try_website_emails), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id})

@app.route("/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status":"not-found"}), 404
    # prepare response
    resp = {
        "status": job.get("status"),
        "total": job.get("total",0),
        "completed": job.get("completed",0),
        "file": (url_for('download', filename=os.path.basename(job["file"])) if job.get("file") else None),
        "keywords": job.get("keywords", {})
    }
    return jsonify(resp)

@app.route("/download/<path:filename>")
def download(filename):
    return send_from_directory(OUT_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    # simple run for development
    app.run(host="0.0.0.0", port=5000, debug=True)

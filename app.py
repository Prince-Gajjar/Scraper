import os
import uuid
import threading
from flask import Flask, request, jsonify, send_from_directory, render_template
from scraper import start_scrape_job

app = Flask(__name__, template_folder="templates", static_folder="static")

OUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT_DIR, exist_ok=True)

jobs = {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/create_job", methods=["POST"])
def create_job():
    data = request.get_json(force=True)
    keyword = data.get("keyword")
    city = data.get("city")
    max_results = int(data.get("max_results", 100))
    agree = data.get("agree", "").lower() in ("true", "1", "yes", "on")

    if not agree:
        return jsonify({"error": "You must agree to the terms"}), 400
    if not keyword or not city:
        return jsonify({"error": "Keyword and city are required"}), 400

    job_id = str(uuid.uuid4())
    out_file = os.path.join(OUT_DIR, f"{job_id}.csv")
    jobs[job_id] = {"status": "running", "count": 0, "file": out_file}

    def run_job():
        try:
            def progress(update):
                jobs[job_id].update(update)
            result = start_scrape_job(keyword, city, max_results, out_file, job_id, progress_callback=progress)
            jobs[job_id]["status"] = "finished"
            jobs[job_id]["count"] = result["count"]
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)

    threading.Thread(target=run_job, daemon=True).start()
    return jsonify({"job_id": job_id})

@app.route("/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"status": "not-found"})
    response = {
        "status": job["status"],
        "count": job.get("count", 0)
    }
    if job["status"] == "finished":
        filename = os.path.basename(job["file"])
        response["download"] = f"/download/{filename}"
    if "error" in job:
        response["error"] = job["error"]
    return jsonify(response)

@app.route("/download/<path:filename>")
def download(filename):
    return send_from_directory(OUT_DIR, filename, as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True, port=5000)

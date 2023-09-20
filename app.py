import datetime, pytz
import os, json
from flask import Flask, render_template, request, url_for, redirect, jsonify, send_file, session
from pymongo import MongoClient
from bson import ObjectId
import pandas as pd

app = Flask(__name__)

app.secret_key = 'strwebapplicationbackend'

client = MongoClient("mongodb://localhost:27017")
db = client["STR"]
dfs = db["dfs"]
files = db["files"]


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        files = dict(request.files.lists())
        for file in files["file"]:
            file_data = file.read()
            return file_data
    else:
        return render_template("index.html")


def extract_dfs(path):
    # code
    dfs = []
    # ...
    return dfs


@app.route("/uploadfile", methods=["GET", "POST"])
def upload():
    if request.method == "POST":
        f = request.files["file"]
        fpath = os.path.join("uploads\\", f.filename)
        f.save(fpath)
        _ts = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
        # _ts = datetime.datetime.utcnow()

        filedata = {
            "name": f.filename,
            "path": fpath,
            "date": _ts,
            "user": 1,
            "status": "Pending",
        }
        files.insert_one(filedata)

        return {"name": f.filename}


@app.route("/filelist")
def filelist():
    flist = list(files.find())
    for f in flist:
        f["_id"] = str(f["_id"])
        f["filetype"] = " "
        f["weekmonth"] = " "

    return jsonify({"data": flist})


# @app.route("/download/<id>")
# def download():


@app.route("/download/<id>")
def download(id):
    file_data = files.find_one({"_id": ObjectId(id)})
    if file_data:
        file_path = file_data["path"]
        return send_file(file_path, as_attachment=True, download_name=file_data["name"])


@app.route("/delete/<id>", methods=["DELETE"])
def delete_file(id):
    file_data = files.find_one({"_id": ObjectId(id)})
    if file_data:
        file_path = file_data["path"]
        os.remove(file_path)
        files.delete_one({"_id": ObjectId(id)})
        return "File deleted successfully"


@app.route("/preview/<id>")
def previewfile(id):
    file_data = files.find_one({"_id": ObjectId(id)})

    if file_data:
        data ={
            'file_id':str(file_data['_id']),
            'file_path': file_data['path']
        }
        return render_template("preview.html", fileData=data)

    else:
        return "File not found"

    # pass


if __name__ == "__main__":
    app.run(debug=True)

import datetime, pytz
import os, json
from flask import Flask, render_template, request, url_for, redirect, jsonify, send_file, session, Response,send_from_directory
from pymongo import MongoClient
from bson import ObjectId
import pandas as pd
import base64
import boto3
import uuid
import io
import mimetypes 
import subprocess


app = Flask(__name__)

app.secret_key = 'strwebapplicationbackend'

client = MongoClient("mongodb://localhost:27017")
db = client["STR"]
dfs = db["dfs"]
files = db["files"]

s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIAZL7AOJLBVBN5MJ2T",
    aws_secret_access_key="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",
    region_name='ap-south-1'
    #  region_name=" "
)
S3_BUCKET_NAME = "hgtech-str-files"
UPLOAD_FOLDER = "uploads\\"



@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        files = dict(request.files.lists())
        for file in files["file"]:
            file_data = file.read()
            return file_data
    else:
        return render_template("index.html")


# def extract_dfs(path):
#     # code
#     dfs = []
#     # ...
#     return dfs


# @app.route("/uploadfile", methods=["GET", "POST"])
# def upload():
#     if request.method == "POST":
#         f = request.files["path"]
#         fpath = os.path.join("uploads\\", f.filename)
#         f.save(fpath)
#         _ts = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
#         # _ts = datetime.datetime.utcnow()

#         filedata = {
#             "name": f.filename,
#             "path": fpath,
#             "date": _ts,
#             "user": 1,
#             "status": "Pending"
#         }
#         files.insert_one(filedata)

#         return {"name": f.filename}

@app.route("/uploadfile", methods=["POST"])
def upload():
    if request.method == "POST":
        f = request.files["path"]
        fpath = os.path.join(UPLOAD_FOLDER, f.filename)
        f.save(fpath)
        _ts = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
        unique_filename = f"{str(uuid.uuid4())}_{f.filename}"

        args = {
            "ContentType": "application/vnd.ms-excel",
            # "ContentDisposition":"attachment"
        }
        try:
            s3.upload_fileobj(Fileobj = f,Bucket = S3_BUCKET_NAME, Key = unique_filename,ExtraArgs=args)
            # s3.put_object
            filedata = {
                "name": f.filename,
                "path": fpath,
                "s3_key": unique_filename,
                "date": _ts, 
                "user": 1,
                "status": "Pending"
            }
            files.insert_one(filedata)
        
            return jsonify({"name": f.filename, "s3_key": unique_filename})
        except Exception as e:
            return {"error": str(e)}



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


# @app.route("/preview/<id>", methods=['POST', 'GET'])
# def previewfile(id):
#     file_data = files.find_one({"_id": ObjectId(id)})
#     if request.method == 'POST':
#         path = file_data['path']
#         filename, file_extension = os.path.splitext(path)

#         if file_extension == '.pdf':
#             with open(path, 'rb') as pdf_file:
#                 pdf_content = pdf_file.read()
#                 pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')
#             return jsonify({'file': pdf_base64, 'ext': file_extension})
    
#         elif(file_extension == '.xlsx'):
#             with open(path, 'rb') as excel_file:
#                 excel_content = excel_file.read()
#                 excel_base64 = base64.b64encode(excel_content).decode('utf-8')
#             return jsonify({'file': excel_base64, 'ext': file_extension}) 
        
           
#     else:
#         return "File not found"

@app.route("/preview/<id>", methods=['GET'])
def previewfile(id):
    file_data = files.find_one({"_id": ObjectId(id)})
    s3_key = file_data["s3_key"]
    signed_url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_key}, 
        ExpiresIn=3600 
    )
    response_data = {"signed_url": signed_url}
    return jsonify(response_data), 200


if __name__ == "__main__":
    app.run(debug=True)

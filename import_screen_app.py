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

libreoffice_path = "C:\\Program Files\\LibreOffice\\program\\soffice.exe"


app = Flask(__name__)

app.secret_key = 'strwebapplicationbackend'

client = MongoClient("mongodb://localhost:27017")
db = client["str3"]
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

def get_file_extension(filename):
    return os.path.splitext(filename)[-1].lower()



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

@app.route("/uploadfile", methods=["POST"])
def upload():
    if request.method == "POST":
        try:
            f = request.files["path"]
            upload_file = f.filename

            fname, ext = os.path.splitext(upload_file)
            fpath = os.path.join(UPLOAD_FOLDER, upload_file)
            f.save(fpath)
            _ts = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
            unique_filename = f"{str(uuid.uuid4())}_{upload_file}"

            file_extension = get_file_extension(upload_file)
            if file_extension == ".xls":
                content_type = "application/vnd.ms-excel"
            elif file_extension == ".xlsx":
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif file_extension == ".pdf":
                content_type = "application/pdf"
            else:
                return jsonify({"error": "Invalid file format. Allowed formats are XLS, XLSX, and PDF."})
            
            # Key_url = f"https://{S3_BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{unique_filename}"

            # responce =s3.put_object(
            #     ACL='public-read',
            #     Body=f,
            #     Bucket=S3_BUCKET_NAME,
            #     Key=unique_filename,
            #     ContentType = content_type
            # )
            s3.upload_fileobj(Fileobj = f,Bucket = S3_BUCKET_NAME, Key = unique_filename,ExtraArgs={"ContentType": content_type})
            

            filedata = {
                "name": upload_file,
                "path": fpath,
                "s3_key": unique_filename,
                # "keyUrl" :Key_url,
                "date": _ts, 
                "user": 1,
                "status": "Pending"
            }

            if ext == '.xlsx':
                path2convert = os.path.join(UPLOAD_FOLDER, "Excel2PDF")
                subprocess.run([libreoffice_path, "--headless", "--convert-to", "pdf", fpath, "--outdir", path2convert])
                pdf_path = os.path.join(path2convert, f"{fname}.pdf")
                filedata["excel2pdf_path"] = pdf_path

            files.insert_one(filedata)

            return jsonify({"name": upload_file , "s3_key": unique_filename})
 
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
        excel2pdf_path = file_data.get("excel2pdf_path", None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        if excel2pdf_path and os.path.exists(excel2pdf_path):
            os.remove(excel2pdf_path)
        files.delete_one({"_id": ObjectId(id)})

        return  "File deleted successfully"
    else:
        return  "File not found"




@app.route("/preview/<id>", methods=['POST', 'GET'])
def previewfile(id):
    file_data = files.find_one({"_id": ObjectId(id)})

    if 'excel2pdf_path' in file_data.keys():
        path = file_data['excel2pdf_path']
    else:
        path = file_data['path']

    file = os.path.basename(path)
    filename, file_extension = os.path.splitext(file)

    with open(path, 'rb') as pdf_file:
        pdf_content = pdf_file.read()
        pdf_base64 = base64.b64encode(pdf_content).decode('utf-8')

    return jsonify({'file': pdf_base64, 'ext': file_extension})




if __name__ == "__main__":
    app.run(debug=True)

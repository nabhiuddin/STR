from flask import jsonify, send_file
import os, json
from bson import ObjectId

# from flask import Flask, render_template, jsonify, url_for, request
from pymongo import MongoClient
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Body, Form, Depends, status,File, UploadFile
from typing import Dict
from starlette.templating import Jinja2Templates
from starlette.requests import Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse,FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware import Middleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import uvicorn
import jwt
import calendar
from pydantic import BaseModel
import boto3
import subprocess
import pandas as pd
import base64
import boto3
import uuid

SECRET_KEY = "$HGT@123"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
libreoffice_path = "C:\\Program Files\\LibreOffice\\program\\soffice.exe"

s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIAZL7AOJLBVBN5MJ2T",
    aws_secret_access_key="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",
    region_name="ap-south-1"
    #  region_name=" "
)
S3_BUCKET_NAME = "hgtech-str-files"
UPLOAD_FOLDER = "uploads\\"


def get_file_extension(filename:str):
    return os.path.splitext(filename)[1].lower()


app = FastAPI()
templates = Jinja2Templates(directory="templates")
# app.mount("/static", StaticFiles(directory="static"))

mongoclient = MongoClient("mongodb://localhost:27017")
db = mongoclient["str4"]

users_db = mongoclient["str_users"]
users_collection = users_db["users"]
files_collection = db["files"]


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.post("/week_data/")
async def get_week_data(data: Dict[str, str] = Body(...)):
    try:
        # data = request.get_json()
        start_date = data["startdate"]
        end_date = data["enddate"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            lookup_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {"$sort": {"timestamp": 1}},
                {
                    "$lookup": {
                        "from": lookup_collection,
                        "localField": "timestamp",
                        "foreignField": "timestamp",
                        "as": "rank_data",
                    }
                },
                {"$unwind": {"path": "$rank_data", "preserveNullAndEmptyArrays": True}},
                {
                    "$match": {
                        "$or": [
                            {"rank_data.metadata.label": "Your rank"},
                            {"rank_data": {"$exists": False}},
                        ]
                    }
                },
                {
                    "$group": {
                        "_id": "$timestamp",
                        "rank": {
                            "$first": {
                                "$concat": [
                                    {
                                        "$toString": {
                                            "$arrayElemAt": ["$rank_data.rank", 0]
                                        }
                                    },
                                    " of ",
                                    {
                                        "$toString": {
                                            "$arrayElemAt": ["$rank_data.rank", 1]
                                        }
                                    },
                                ]
                            }
                        },
                        "data": {
                            "$push": {
                                "label": "$metadata.label",
                                "change": "$change",
                                "change_rate": "$change_rate",
                            }
                        },
                    }
                },
                {"$project": {"_id": 0, "timestamp": "$_id", "rank": 1, "data": 1}},
                {"$sort": {"timestamp": 1}},
            ]

            collection = db[collection_name]

            result.update({collection_name: list(collection.aggregate(pipeline))})

        # print(result)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/month_data/")
async def get_month_data(data: Dict[str, str] = Body(...)):
    try:
        # print(data)
        # data = request.get_json()
        year = int(data["year"])
        month = int(data["month"])
        # day = datetime.now().day
        year_upto = year + 1
        from_year = year_upto - 3

        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        selected_3months_result = {}
        total_yearToDate_result = {}
        total_Running3Month_result = {}
        total_Running12Month_result = {}
        final_result = {}

        selected_month = month
        selected_year = year
        for loop in range(3):
            result1 = {}

            if month >= 3:
                print(selected_month, selected_year)
            else:
                print(selected_month, selected_year)
            for collection_name in collection_rank_mapping.keys():
                rank_collection = collection_rank_mapping[collection_name]
                toYear = selected_year
                toMonth = selected_month
                max_days = calendar.monthrange(toYear, toMonth)[1]
                toDay = max_days
                fromYear = selected_year
                fromMonth = selected_month
                fromDay = 1

                pipeline = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "month": {"$month": "$timestamp"},
                                "label": "$metadata.label",
                            },
                            "avg_change": {"$avg": "$change"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": {
                                "$dateFromParts": {
                                    "year": "$_id.year",
                                    "month": "$_id.month",
                                }
                            },
                            "label": "$_id.label",
                            "avg_change": "$avg_change",
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]

                pipeline1 = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "year": {"$year": "$timestamp"},
                                "month": {"$month": "$timestamp"},
                            },
                            "rank_avg_numerator": {
                                "$avg": {"$arrayElemAt": ["$rank", 0]}
                            },
                            "rank_avg_denominator": {
                                "$avg": {"$arrayElemAt": ["$rank", 1]}
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "month": {
                                "$dateFromParts": {
                                    "year": "$_id.year",
                                    "month": "$_id.month",
                                }
                            },
                            "month_rank_avg": {
                                "$concat": [
                                    {"$toString": {"$round": "$rank_avg_numerator"}},
                                    " of ",
                                    {"$toString": {"$round": "$rank_avg_denominator"}},
                                ]
                            },
                        }
                    },
                ]

                collection = db[collection_name]
                rank_collection = db[rank_collection]
                change_coll = collection_name + "_" + "change"
                rank_coll = collection_name + "_" + "ranks"
                result_here = {}
                result_here.update({change_coll: list(collection.aggregate(pipeline))})
                result_here.update(
                    {rank_coll: list(rank_collection.aggregate(pipeline1))}
                )
                result1.update({collection_name: result_here})
            month_names = {
                1: "Jan",
                2: "Feb",
                3: "Mar",
                4: "Apr",
                5: "May",
                6: "Jun",
                7: "Jul",
                8: "Aug",
                9: "Sep",
                10: "Oct",
                11: "Nov",
                12: "Dec",
            }
            selected_month_name = month_names[selected_month]
            selected_3months_result.update({selected_month_name: result1})
            if selected_month == 1:
                selected_month = 12
                selected_year -= 1
            else:
                selected_month -= 1
        final_result.update({year: selected_3months_result})

        for loopingYear in range(from_year, year_upto):
            one_yearToDate_result = {}

            for collection_name in collection_rank_mapping.keys():
                rank_collection = collection_rank_mapping[collection_name]
                fromYear = loopingYear
                fromMonth = 1
                fromDay = 1
                toYear = loopingYear
                toMonth = datetime.now().month
                toDay = datetime.now().day

                pipeline = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            "_id": {"label": "$metadata.label"},
                            "avg_change": {"$avg": "$change"},
                            "timestamp": {"$last": "$timestamp"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": 1,
                            "label": "$_id.label",
                            "avg_change": "$avg_change",
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]

                pipeline1 = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            # "_id": {
                            #     "year": {"$year": "$timestamp"},
                            #     # "month": {"$month": "$timestamp"}
                            # },
                            "_id": None,
                            "timestamp": {"$last": "$timestamp"},
                            "rank_avg_numerator": {
                                "$avg": {"$arrayElemAt": ["$rank", 0]}
                            },
                            "rank_avg_denominator": {
                                "$avg": {"$arrayElemAt": ["$rank", 1]}
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": "$timestamp",
                            # "year": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #         # "month": "$_id.month"
                            #     }
                            # },
                            "year_rank_avg": {
                                "$concat": [
                                    {"$toString": {"$round": "$rank_avg_numerator"}},
                                    " of ",
                                    {"$toString": {"$round": "$rank_avg_denominator"}},
                                ]
                            },
                        }
                    },
                ]

                collection = db[collection_name]
                rank_collection = db[rank_collection]
                change_coll = collection_name + "_" + "change"
                rank_coll = collection_name + "_" + "ranks"
                result_here = {}
                result_here.update({change_coll: list(collection.aggregate(pipeline))})
                result_here.update(
                    {rank_coll: list(rank_collection.aggregate(pipeline1))}
                )
                one_yearToDate_result.update({collection_name: result_here})
            total_yearToDate_result.update({loopingYear: one_yearToDate_result})
        final_result.update({"Year To Date": total_yearToDate_result})

        for loopingYear in range(from_year, year_upto):
            one_Running3Month_result = {}

            for collection_name in collection_rank_mapping.keys():
                rank_collection = collection_rank_mapping[collection_name]

                toYear = loopingYear
                toMonth = datetime.now().month
                max_days = calendar.monthrange(toYear, toMonth)[1]
                toDay = max_days

                if toMonth >= 3:
                    fromYear = loopingYear
                    fromMonth = toMonth - 2
                    fromDay = 1
                else:
                    fromYear = loopingYear - 1
                    fromMonth = 10 + toMonth
                    fromDay = 1

                # print(f"year: {year}, month: {month}, toMonth: {toMonth}, toDay: {toDay} , strid:{str_id_objId}")
                pipeline = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                # "year": {"$year": "$timestamp"},
                                "label": "$metadata.label"
                            },
                            "avg_change": {"$avg": "$change"},
                            "timestamp": {"$last": "$timestamp"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": 1,
                            # "timestamp": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #     }
                            # },
                            "label": "$_id.label",
                            "avg_change": "$avg_change",
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]

                pipeline1 = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            # "_id": {
                            #     "year": {"$year": "$timestamp"},
                            #     # "month": {"$month": "$timestamp"}
                            # },
                            "_id": None,
                            "timestamp": {"$last": "$timestamp"},
                            "rank_avg_numerator": {
                                "$avg": {"$arrayElemAt": ["$rank", 0]}
                            },
                            "rank_avg_denominator": {
                                "$avg": {"$arrayElemAt": ["$rank", 1]}
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            # "year": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #         # "month": "$_id.month"
                            #     }
                            # },
                            "timestamp": "$timestamp",
                            "year_rank_avg": {
                                "$concat": [
                                    {"$toString": {"$round": "$rank_avg_numerator"}},
                                    " of ",
                                    {"$toString": {"$round": "$rank_avg_denominator"}},
                                ]
                            },
                        }
                    },
                ]

                collection = db[collection_name]
                rank_collection = db[rank_collection]
                change_coll = collection_name + "_" + "change"
                rank_coll = collection_name + "_" + "ranks"
                result_here = {}
                result_here.update({change_coll: list(collection.aggregate(pipeline))})
                result_here.update(
                    {rank_coll: list(rank_collection.aggregate(pipeline1))}
                )
                one_Running3Month_result.update({collection_name: result_here})
            total_Running3Month_result.update({loopingYear: one_Running3Month_result})
        final_result.update({"Running 3 Month": total_Running3Month_result})

        for loopingYear in range(from_year, year_upto):
            one_Running12Month_result = {}

            for collection_name in collection_rank_mapping.keys():
                rank_collection = collection_rank_mapping[collection_name]

                toYear = loopingYear
                toMonth = datetime.now().month
                max_days = calendar.monthrange(toYear, toMonth)[1]
                toDay = max_days

                if toMonth == 12:
                    fromYear = loopingYear
                    fromMonth = 1
                    fromDay = 1
                else:
                    fromYear = loopingYear - 1
                    fromMonth = toMonth + 1
                    fromDay = 1

                # print(f"year: {year}, month: {month}, toMonth: {toMonth}, toDay: {toDay} , strid:{str_id_objId}")
                pipeline = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                # "year": {"$year": "$timestamp"},
                                "label": "$metadata.label"
                            },
                            "avg_change": {"$avg": "$change"},
                            "timestamp": {"$last": "$timestamp"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "timestamp": 1,
                            # "timestamp": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #     }
                            # },
                            "label": "$_id.label",
                            "avg_change": "$avg_change",
                        }
                    },
                    {"$sort": {"timestamp": 1}},
                ]

                pipeline1 = [
                    {
                        "$match": {
                            "timestamp": {
                                "$gte": datetime(fromYear, fromMonth, fromDay),
                                "$lte": datetime(toYear, toMonth, toDay),
                            },
                            "metadata.str_id": ObjectId(str_id_objId),
                        }
                    },
                    {
                        "$group": {
                            # "_id": {
                            #     "year": {"$year": "$timestamp"},
                            #     # "month": {"$month": "$timestamp"}
                            # },
                            "_id": None,
                            "timestamp": {"$last": "$timestamp"},
                            "rank_avg_numerator": {
                                "$avg": {"$arrayElemAt": ["$rank", 0]}
                            },
                            "rank_avg_denominator": {
                                "$avg": {"$arrayElemAt": ["$rank", 1]}
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            # "year": {
                            #     "$dateFromParts": {
                            #         "year": "$_id.year",
                            #         # "month": "$_id.month"
                            #     }
                            # },
                            "timestamp": "$timestamp",
                            "year_rank_avg": {
                                "$concat": [
                                    {"$toString": {"$round": "$rank_avg_numerator"}},
                                    " of ",
                                    {"$toString": {"$round": "$rank_avg_denominator"}},
                                ]
                            },
                        }
                    },
                ]

                collection = db[collection_name]
                rank_collection = db[rank_collection]
                change_coll = collection_name + "_" + "change"
                rank_coll = collection_name + "_" + "ranks"
                result_here = {}
                result_here.update({change_coll: list(collection.aggregate(pipeline))})
                result_here.update(
                    {rank_coll: list(rank_collection.aggregate(pipeline1))}
                )
                one_Running12Month_result.update({collection_name: result_here})
            total_Running12Month_result.update({loopingYear: one_Running12Month_result})
        final_result.update({"Running 12 Month": total_Running12Month_result})

        return final_result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/weekly_data/")
async def get_weekly_data(data: Dict[str, str] = Body(...)):
    try:
        # data = request.get_json()
        week_start_date = data["week_start_date"]
        week_end_date = data["week_end_date"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(week_start_date),
                            "$lte": datetime.fromisoformat(week_end_date),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "week": {"$week": "$timestamp"},
                            "label": "$metadata.label",
                        },
                        "avg_change": {"$avg": "$change"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "week_start_date": {
                            "$dateFromParts": {
                                "isoWeekYear": "$_id.year",
                                "isoWeek": "$_id.week",
                            }
                        },
                        "label": "$_id.label",
                        "avg_change": 1,
                    }
                },
                {"$sort": {"week_start_date": 1}},
                {
                    "$group": {
                        "_id": "$label",
                        "weekly_averages": {"$push": "$$ROOT"},
                        "total_weeks_average": {"$avg": "$avg_change"},
                    }
                },
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(week_start_date),
                            "$lte": datetime.fromisoformat(week_end_date),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "week": {"$week": "$timestamp"},
                        },
                        "rank_avg_numerator": {"$avg": {"$arrayElemAt": ["$rank", 0]}},
                        "rank_avg_denominator": {
                            "$avg": {"$arrayElemAt": ["$rank", 1]}
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "timestamp": {
                            "$dateFromParts": {
                                "isoWeekYear": "$_id.year",
                                "isoWeek": "$_id.week",
                                # "isoDayOfWeek": 0
                            }
                        },
                        "weekly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        },
                        "rank_avg_numerator": "$rank_avg_numerator",
                        "rank_avg_denominator": "$rank_avg_denominator",
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "weekly_rank_avg": {"$push": "$$ROOT"},
                        "rank_avg_numerator": {"$avg": "$rank_avg_numerator"},
                        "rank_avg_denominator": {"$avg": "$rank_avg_denominator"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "weekly_rank_avg": "$weekly_rank_avg",
                        "total_weekly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        },
                    }
                },
            ]
            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name + "_" + "change"
            rank_coll = collection_name + "_" + "ranks"
            result_here = []
            result_here.append({change_coll: list(collection.aggregate(pipeline))})
            result_here.append({rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})
        # print(result)
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/monthly_data/")
async def get_monthly_data(data: Dict[str, str] = Body(...)):
    try:
        # data = request.get_json()
        year = int(data["year_selected"])
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]
            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(year, 1, 1),
                            "$lt": datetime(year + 1, 1, 1),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                            "label": "$metadata.label",
                        },
                        "avg_change": {"$avg": "$change"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month",
                            }
                        },
                        "label": "$_id.label",
                        "avg_change": 1,
                    }
                },
                {"$sort": {"month": 1}},
                {
                    "$group": {
                        "_id": "$label",
                        "monthly_averages": {"$push": "$$ROOT"},
                        "total_months_average": {"$avg": "$avg_change"},
                    }
                },
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(year, 1, 1),
                            "$lt": datetime(year + 1, 1, 1),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "month": {"$month": "$timestamp"},
                        },
                        "rank_avg_numerator": {"$avg": {"$arrayElemAt": ["$rank", 0]}},
                        "rank_avg_denominator": {
                            "$avg": {"$arrayElemAt": ["$rank", 1]}
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "month": {
                            "$dateFromParts": {
                                "year": "$_id.year",
                                "month": "$_id.month",
                            }
                        },
                        "monthly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        },
                        "rank_avg_numerator": "$rank_avg_numerator",
                        "rank_avg_denominator": "$rank_avg_denominator",
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "monthly_rank_avg": {"$push": "$$ROOT"},
                        "rank_avg_numerator": {"$avg": "$rank_avg_numerator"},
                        "rank_avg_denominator": {"$avg": "$rank_avg_denominator"},
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "monthly_rank_avg": "$monthly_rank_avg",
                        "total_monthly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        },
                    }
                },
            ]

            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name + "_" + "change"
            rank_coll = collection_name + "_" + "ranks"
            result_here = []
            result_here.append({change_coll: list(collection.aggregate(pipeline))})
            result_here.append({rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/yearly_data/")
async def get_yearly_data(data: Dict[str, str] = Body(...)):
    try:
        # data = request.get_json()
        num = int(data["years_selected"])
        current_year = datetime.now().year
        start_year = current_year - num
        # print(current_year,start_year)
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]
        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]

            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(start_year, 1, 1),
                            "$lte": datetime(current_year, 10, 1),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "year": {"$year": "$timestamp"},
                            "label": "$metadata.label",
                        },
                        "avg_change": {"$avg": "$change"},
                    }
                },
                {"$sort": {"year": -1}},
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime(start_year, 1, 1),
                            "$lte": datetime(current_year, 12, 1),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {"year": {"$year": "$timestamp"}},
                        "rank_avg_numerator": {"$avg": {"$arrayElemAt": ["$rank", 0]}},
                        "rank_avg_denominator": {
                            "$avg": {"$arrayElemAt": ["$rank", 1]}
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "year": {"$dateFromParts": {"year": "$_id.year"}},
                        "yearly_rank_avg": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        },
                    }
                },
            ]
            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name + "_" + "change"
            rank_coll = collection_name + "_" + "ranks"
            result_here = []
            result_here.append({change_coll: list(collection.aggregate(pipeline))})
            result_here.append({rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)

        return result
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/range_data/")
async def get_range_data(data: Dict[str, str] = Body(...)):
    try:
        # data = request.get_json()
        start_date = data["startdate"]
        end_date = data["enddate"]
        str_id = data["str_id"]
        obj = db.str_reports.find_one({"str_id": str_id})
        # print(obj)
        str_id_objId = obj["_id"]

        collection_rank_mapping = {
            "adr": "adr_ss_ranks",
            "occupancy": "occupancy_ss_ranks",
            "revpar": "revpar_ss_ranks",
        }

        result = {}

        for collection_name in collection_rank_mapping.keys():
            rank_collection = collection_rank_mapping[collection_name]
            pipeline = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": {"label": "$metadata.label"},
                        "avg_change": {"$avg": "$change"},
                    }
                },
                {
                    "$project": {
                        "label": "$_id.label",
                        "avg_change": "$avg_change",
                        "_id": 0,
                    }
                },
            ]

            pipeline1 = [
                {
                    "$match": {
                        "timestamp": {
                            "$gte": datetime.fromisoformat(start_date),
                            "$lte": datetime.fromisoformat(end_date),
                        },
                        "metadata.str_id": ObjectId(str_id_objId),
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "rank_avg_numerator": {"$avg": {"$arrayElemAt": ["$rank", 0]}},
                        "rank_avg_denominator": {
                            "$avg": {"$arrayElemAt": ["$rank", 1]}
                        },
                    }
                },
                {
                    "$project": {
                        "rank": {
                            "$concat": [
                                {"$toString": {"$round": "$rank_avg_numerator"}},
                                " of ",
                                {"$toString": {"$round": "$rank_avg_denominator"}},
                            ]
                        }
                    }
                },
            ]

            collection = db[collection_name]
            rank_collection = db[rank_collection]
            change_coll = collection_name + "_" + "change"
            rank_coll = collection_name + "_" + "ranks"
            result_here = []
            result_here.append({change_coll: list(collection.aggregate(pipeline))})
            result_here.append({rank_coll: list(rank_collection.aggregate(pipeline1))})
            result.update({collection_name: result_here})

        # print(result)

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str = None


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(
    token: str = Depends(OAuth2PasswordBearer(tokenUrl="token")),
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except jwt.PyJWTError:
        raise credentials_exception
    return token_data


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = users_collection.find_one({"username": form_data.username})
    if user is None or form_data.password != user["password"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/dashboard", response_class=HTMLResponse)
async def index(request: Request):
    # return FileResponse("templates/index.html")
    return templates.TemplateResponse("dashboard.html", context={"request": request})


@app.get("/registration", response_class=HTMLResponse)
async def registration(request: Request):
    return templates.TemplateResponse("registration.html", {"request": request})


@app.post("/toRegister")
async def registerToDb(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username
    password = form_data.password
    user = users_collection.find_one({"username": username})
    if user != None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Username already registered.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    else:
        users_collection.insert_one({"username": username, "password": password})
        return {"message": "registered succesfully"}


@app.get("/", response_class=HTMLResponse)
async def login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/index", response_class=HTMLResponse)
async def indexs(request: Request):
    if request.method == "POST":
        files = dict(request.files_collection.lists())
        for file in files["file"]:
            file_data = file.read()
            return file_data
    else:
        return templates.TemplateResponse("index.html", {"request": request})


@app.post("/uploadfile")
def upload(path: UploadFile, request: Request):
    try:
        content = path.file
        upload_file = path.filename
        fname, ext = os.path.splitext(upload_file)
        fpath = os.path.join(UPLOAD_FOLDER, upload_file)

        with open(fpath, 'wb') as file:
            file.write(content.read())

        _ts = datetime.now()
        print(_ts)
        unique_filename = f"{str(uuid.uuid4())}_{upload_file}"

        file_extension = get_file_extension(upload_file)

        if file_extension == ".xls":
            content_type = "application/vnd.ms-excel"
        elif file_extension == ".xlsx":
            content_type = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        elif file_extension == ".pdf":
            content_type = "application/pdf"
        else:
            return JSONResponse(
                {
                    "error": "Invalid file format. Allowed formats are XLS, XLSX, and PDF."
                }
            )

        # Key_url = f"https://{S3_BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{unique_filename}"

        # responce =s3.put_object(
        #     ACL='public-read',
        #     Body=f,
        #     Bucket=S3_BUCKET_NAME,
        #     Key=unique_filename,
        #     ContentType = content_type
        # )
        s3.upload_fileobj(
            Fileobj=content,
            Bucket=S3_BUCKET_NAME,
            Key=unique_filename,
            ExtraArgs={"ContentType": content_type},
        )

        filedata = {
            "name": upload_file,
            "path": fpath,
            "s3_key": unique_filename,
            # "keyUrl" :Key_url,
            "date": _ts,
            "user": 1,
            "status": "Pending",
        }

        if ext == ".xlsx":
            path2convert = os.path.join(UPLOAD_FOLDER, "Excel2PDF")
            subprocess.run(
                [
                    libreoffice_path,
                    "--headless",
                    "--convert-to",
                    "pdf",
                    fpath,
                    "--outdir",
                    path2convert,
                ]
            )
            pdf_path = os.path.join(path2convert, f"{fname}.pdf")
            filedata["excel2pdf_path"] = pdf_path

        files_collection.insert_one(filedata)

        return JSONResponse({"name": upload_file, "s3_key": unique_filename})

    except Exception as e:
        return JSONResponse({"error": str(e)})


@app.get("/filelist", response_class=JSONResponse)
def filelist(request: Request):
    flist = list(files_collection.find())

    for f in flist:
        f["_id"] = str(f["_id"])
        f['date'] = str(f['date'])
        f["filetype"] = " "
        f["weekmonth"] = " "
    
    return JSONResponse({"data": flist})


@app.get("/download/{id}")
def download(request: Request, id: str):
    file_data = files_collection.find_one({"_id": ObjectId(id)})
    if file_data:
        file_path = file_data["path"]
        # return send_file(file_path, as_attachment=True, download_name=file_data["name"])
        return FileResponse(file_path, headers={"Content-Disposition": f"attachment; filename={file_data['name']}"})


@app.delete("/delete/{id}")
def delete_file(request: Request, id: str):
    file_data = files_collection.find_one({"_id": ObjectId(id)})

    if file_data:
        file_path = file_data["path"]
        excel2pdf_path = file_data.get("excel2pdf_path", None)
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
        if excel2pdf_path and os.path.exists(excel2pdf_path):
            os.remove(excel2pdf_path)
        files_collection.delete_one({"_id": ObjectId(id)})

        return "File deleted successfully"
    else:
        return "File not found"


@app.post("/preview/{id}", response_class=JSONResponse)
def previewfile(request: Request, id: str):
    file_data = files_collection.find_one({"_id": ObjectId(id)})

    if "excel2pdf_path" in file_data.keys():
        path = file_data["excel2pdf_path"]
    else:
        path = file_data["path"]

    file = os.path.basename(path)
    filename, file_extension = os.path.splitext(file)

    with open(path, "rb") as pdf_file:
        pdf_content = pdf_file.read()
        pdf_base64 = base64.b64encode(pdf_content).decode("utf-8")

    return JSONResponse({"file": pdf_base64, "ext": file_extension})


# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8000)

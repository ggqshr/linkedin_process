# Extract de-duplicated work experience to new collection
# -*- coding:utf-8 -*-

import pymongo
from pymongo.errors import DuplicateKeyError
import json


ORINGINAL = 'results.US_expand_0-600'
OBJECT = 'US_exp_' + ORINGINAL.split("_")[-1]

client = pymongo.MongoClient()

ori_db = client['US_expand']
ori_cl = ori_db[ORINGINAL]

db = client['US_experience_ggq']
obj_cl = db[OBJECT]

cp_db = client["companies"]
acc_cl = db["acc50_withurn"]


ACC_50 = set()
for item in acc_cl.find():
    ACC_50.add(item.get("companyUrn"))


try:
    for i, row in enumerate(ori_cl.find({})):
        print(i)
        experience = []
        data = json.loads(row.get("profile"))
        person_id = row.get("public_identifier")
        positions_info = data.get("positions_info")

        if positions_info:
            positions_info.reverse()
            join50acc = "bef"
            bef_companyUrn = "0"
            accStartDate = ""

            for n, each in enumerate(positions_info):
                companyUrn = each.get("companyUrn")
                item = {}
                if companyUrn:
                    companyUrn = companyUrn.split(":")[-1]
                    item["person_id"] = person_id
                    item["companyUrn"] = companyUrn
                    item["title"] = each.get("title")

                    startDate = each.get("startDate")
                    if startDate:
                        startYear = startDate.get("year")
                        startMonth = startDate.get("month")
                        startYear = startYear if startYear else 0
                        startMonth = startMonth if startMonth else 0
                    else:
                        startYear = 0
                        startMonth = 0
                    item["startDate"] = str(startYear) + '/' + str(startMonth)

                    endDate = each.get("endDate")
                    if endDate:
                        endYear = endDate.get("year")
                        endMonth = endDate.get("month")
                        endYear = endYear if endYear else 0
                        endMonth = endMonth if endMonth else 0
                    else:
                        endYear = 0
                        endMonth = 0
                    item["endDate"] = str(endYear) + '/' + str(endMonth)

                    if companyUrn == bef_companyUrn:
                        pass

                    elif join50acc == "cur":
                        join50acc = "aft"

                    elif join50acc == "bef" and companyUrn in ACC_50:
                        join50acc = "cur"
                        accStartDate = item["startDate"]

                    bef_companyUrn = companyUrn
                    item["join50acc"] = join50acc
                    if accStartDate:
                        item["accStartDate"] = accStartDate

                    obj_cl.insert(item)


finally:
    client.close()

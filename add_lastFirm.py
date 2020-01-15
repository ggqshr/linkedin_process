# -*- coding:utf-8 -*-

import pymongo

client = pymongo.MongoClient()
db = client["companies"]
oricl = db["acc50_withurn"]

db2 = client["US_experience"]
cl_aft = db2["aft"]
cl_cur = db2["cur"]
cl_exp = db2["Russell1718_exp_after_acc50"]


def get50urn():
    urn = set()
    for item in oricl.find():
        urn.add(item.get("companyUrn"))
    return urn


def update_accJob_sign():
    domain_list = get50urn()
    for item in cl_aft.find():
        _id = item.get("_id")
        cU = item.get("companyUrn")
        if cU in domain_list:
            cl_aft.update({"_id":_id},{"$set":{"accJob":True}})


def add_last():
    c = cl_exp.count({"lastAccFirmUrn": {"$exists": False}})
    # Add three Field, including: lastAccFirm, lastAccFirmUrn, accFinishDate
    for n, i in enumerate(cl_exp.find({"lastAccFirmUrn": {"$exists": False}}, no_cursor_timeout=True)):
        print(c-n+1)
        _id = i.get("_id")
        pid = i.get("person_id")
        current_title = i.get("title")
        current_companyUrn = i.get("companyUrn")
        current_endDate = i.get("endDate")
        current_accJob = i.get("accJob")
        exp_list = []
        for item in cl_cur.find({"person_id":pid}):
            exp_list.append((item.get("companyUrn"), item.get("title"), item.get("endDate"), True))
        for item in cl_aft.find({"person_id":pid}):
            exp_list.append((item.get("companyUrn"), item.get("title"), item.get("endDate"), item.get("accJob")))
        current_index = exp_list.index((current_companyUrn, current_title, current_endDate, current_accJob))

        # Find the latest acc experience
        lastAccFirmUrn, accFinishDate = "0", "0"
        for j in range(current_index-1, -1, -1):
            if exp_list[j][3]:
                lastAccFirmUrn, accFinishDate = exp_list[j][0], exp_list[j][2]
                break

        lastAccFirm = oricl.find_one({"companyUrn":lastAccFirmUrn}).get("company name")
        if lastAccFirm:
            cl_exp.update({"_id": _id}, {"$set": {
                "lastAccFirm":lastAccFirm, "lastAccFirmUrn": lastAccFirmUrn, "accFinishDate": accFinishDate}
            })


if __name__ == "__main__":
    # update_accJob_sign()
    add_last()

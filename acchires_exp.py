# -*- coding:utf-8 -*-

import pymongo


client = pymongo.MongoClient()

db = client["US_experience"]
cl_updated_aft = db["Russell1718_exp_after_acc50"]

cur_cl = db["cur"]
aft_cl = db["aft"]

accHires_exp = db["accHires_exp_ggq"]

def find_accHires_id():
    person_id = set()
    accStartYearRange = [2002, 2009]
    for item in cl_updated_aft.find(no_cursor_timeout=False):
        pid, accStartDate = item.get("person_id"), item.get("accStartDate")
        if accStartDate >= accStartYearRange[0] and accStartDate <= accStartYearRange[1]:
            person_id.add(pid)
    return person_id


def exp_after_accjob(ids):
    for pid in ids:
        for cur_exp in cur_cl.find({"person_id": pid}):
            accHires_exp.insert_one(cur_exp)
        for aft_exp in aft_cl.find({"person_id": pid}):
            accHires_exp.insert_one(aft_exp)


if __name__ == "__main__":
    try:
        ids = find_accHires_id()
        exp_after_accjob(ids)
    finally:
        client.close()
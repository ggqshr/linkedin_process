# -*- coding:utf-8 -*-

import pymongo


client = pymongo.MongoClient()

db = client["US_experience"]
cl_updated_aft = db["Russell1718_exp_after_acc50"]

aft_cl = db["aft"]

after_Rus3000_exp = db["after_Rus3000_exp_ggq"]

db_cp = client["companies"]
cp_cl = db_cp["companyInfo"]

def load_exp_by_pid(pid):
    person_exp = []
    for item in aft_cl.find({"person_id": pid}):
        person_exp.append(item)
    return person_exp

def datestr1_later_equal_than_datestr2(datestr1, datestr2):
    datestr1_year, datestr1_month = datestr1.split("/")
    datestr2_year, datestr2_month = datestr2.split("/")
    if int(datestr1_year) > int(datestr2_year):
        return True
    elif datestr1_year == datestr2_year:
        if datestr1_month == "0":
            return True
        elif int(datestr1_month) >= int(datestr2_month):
            return True
    else:
        return False


def find_next_cp():
    tmp_person_id = ""
    for item in cl_updated_aft.find():
        pid = item.get("person_id")
        current_startDate, current_endDate, current_urn = item.get("startDate"), item.get("endDate"), item.get("companyUrn")

        if pid != tmp_person_id:
            person_exp = load_exp_by_pid(pid)

        for index, each_exp in enumerate(person_exp):
            tmp_startDate, tmp_urn = each_exp.get("startDate"), each_exp.get("companyUrn")
            if tmp_startDate == current_startDate and tmp_urn == current_urn:
                tmp_index = index

        next_exp = []
        for tmp_next_exp in person_exp[tmp_index+1:]:
            if not next_exp:
                tmp_next_startDate = tmp_next_exp.get("startDate")
            elif tmp_next_startDate != tmp_next_exp.get("startDate"):
                break
            if datestr1_later_equal_than_datestr2(tmp_next_startDate, current_endDate):
                next_exp.append(tmp_next_exp)

        if len(next_exp) >= 1:
            if len(next_exp) > 1:
                item["dup"] = 1
            for e in next_exp:
                item["nextCompanyUrn"] = e.get("companyUrn")
                item["nextStartDate"] = e.get("startDate")
                item.pop("_id")
                after_Rus3000_exp.insert_one(item)

        else:
            item.pop("_id")
            after_Rus3000_exp.insert_one(item)


def write_companyName_by_urn():
    for item in after_Rus3000_exp.find():
        ncu = item.get("nextCompanyUrn")
        _id = item.get("_id")
        if ncu:
            cp_item = cp_cl.find_one({"companyUrn":ncu})
            if cp_item:
                un = cp_item.get("universalName")
                after_Rus3000_exp.update_one({"_id":_id},{"$set":{"nextCompanyName":un}})

if __name__ == "__main__":
    try:
        find_next_cp()
        write_companyName_by_urn()
    finally:
        client.close()
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


SENIOR = False

initYear = 2000
stopYear = 2018

client = MongoClient()
db = client["US_experience"]
cl_updated_aft = db["Russell1718_exp_after_acc50"]

cl_init_cnt1 = db["init_cnt_in" + str(initYear)]
cl_mobility_cnt1 = db["mobility_cnt"]
cl_init_cnt2 = db["init_senior_cnt_in" + str(initYear)]
cl_mobility_cnt2 = db["mobility_senior_cnt"]

if SENIOR:
    cl_init_cnt = cl_init_cnt2
    cl_mobility_cnt = cl_mobility_cnt2
else:
    cl_init_cnt = cl_init_cnt1
    cl_mobility_cnt = cl_mobility_cnt1


def gen_cl_init_cnt():
    urn_init_cnt_dict = {}

    if SENIOR:
        cursor = cl_updated_aft.find({"seniorManagers": True}, no_cursor_timeout=False)
    else:
        cursor = cl_updated_aft.find(no_cursor_timeout=False)

    for item in cursor:
        companyUrn = item.get("companyUrn")
        if companyUrn not in urn_init_cnt_dict.keys():
            urn_init_cnt_dict[companyUrn] = 0
        startYear = item.get("startDate").split("/")[0]
        endYear = item.get("endDate").split("/")[0]

        if int(startYear) < initYear and int(startYear) != 0:
            if int(endYear) >= initYear or int(endYear) == 0:
                urn_init_cnt_dict[companyUrn] += 1
                print(item.get("startDate"), item.get("endDate"))

    for i in urn_init_cnt_dict.items():
        cl_init_cnt.insert(dict(companyUrn=i[0], initCnt=i[1]))


def gen_cl_mobility_cnt():


    if SENIOR:
        cursor1 = cl_updated_aft.distinct("companyUrn", {"seniorManagers":True})
    else:
        cursor1 = cl_updated_aft.distinct("companyUrn")

    for n, companyUrn in enumerate(cursor1):
        print(n, companyUrn)
        incomingCnt = {}
        outgoingCnt = {}

        if SENIOR:
            cursor2 = cl_updated_aft.find({"companyUrn": companyUrn, "seniorManagers": True}, no_cursor_timeout=False)
        else:
            cursor2 = cl_updated_aft.find({"companyUrn": companyUrn}, no_cursor_timeout=False)

        for item in cursor2:
            startDate = item.get("startDate")
            year, month = startDate.split("/")
            if int(year) >= initYear or int(year) == 0:
                if startDate not in incomingCnt.keys():
                    incomingCnt[startDate] = 1
                else:
                    incomingCnt[startDate] += 1

            endDate = item.get("endDate")
            year, month = endDate.split("/")
            if int(year) >= initYear or int(year) == 0:
                if endDate not in outgoingCnt.keys():
                    outgoingCnt[endDate] = 1
                else:
                    outgoingCnt[endDate] += 1

        for row in creat_redundant_date(incomingCnt, outgoingCnt):
            cl_mobility_cnt.insert(
                dict(companyUrn=companyUrn, year=row[0], month=row[1], incoming=row[2], outgoing=row[3])
            )


def creat_redundant_date(inCnt, outCnt):
    year_arr = [year for year in range(initYear, stopYear+1)]
    year_arr.append(0)
    month_arr = [month for month in range(1, 13)]
    month_arr.insert(6, 0)
    date_in_out = []

    for year in year_arr:
        for month in month_arr:
            date_str = str(year)+"/"+str(month)
            if date_str == "0/1":
                break
            in_v = inCnt.get(date_str) if inCnt.get(date_str) else 0
            out_v = outCnt.get(date_str) if outCnt.get(date_str) else 0
            yield (year, month, in_v, out_v)

    return date_in_out


def update_acc_num():
    tmpUrn = 0
    accNum = 0
    print("Updating...")
    for item in cl_mobility_cnt.find():
        _id = item.get("_id")
        curUrn = item.get("companyUrn")
        incoming = item.get("incoming")
        outgoing = item.get("outgoing")
        if tmpUrn != curUrn:
            try:
                accNum = cl_init_cnt.find_one({"companyUrn": curUrn}).get("initCnt")
            except:
                pass
            tmpUrn = curUrn
        accNum = accNum + incoming - outgoing
        cl_mobility_cnt.update({"_id": _id}, {"$set": {"accNum": accNum}})
    print("Finish")


if __name__ == "__main__":
    try:
        gen_cl_init_cnt()
        gen_cl_mobility_cnt()
        update_acc_num()
    finally:
        client.close()

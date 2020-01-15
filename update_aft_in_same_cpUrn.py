from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


client = MongoClient()

db = client["US_experience"]
cl_init_aft = db["aft(dup)"]
cl_updated_aft = db["aft"]


try:
    item1 = cl_init_aft.find_one()

    for n,item in enumerate(cl_init_aft.find()):
        print(n)

        item1_cpUrn = item1.get("companyUrn")
        item1_pid = item1.get("person_id")
        item1_startDate = item1.get("startDate")
        item1_endDate = item1.get("endDate")

        item2_cpUrn = item.get("companyUrn")
        item2_pid = item.get("person_id")
        if item2_cpUrn == item1_cpUrn and item2_pid == item1_pid:
            item1["endDate"] = item.get("endDate")
        else:
            cl_updated_aft.insert(item1)
            item1 = item

    # finish the last item
    if item2_cpUrn == item1_cpUrn and item2_pid == item1_pid:
        cl_updated_aft.insert(item1)


finally:
    client.close()
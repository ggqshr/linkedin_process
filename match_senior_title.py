from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from collections import Counter

client = MongoClient()

db = client["US_experience"]
cl_aft = db["aft"]
cl = db["Russell1718_exp_after_acc50"]
cl_kw = db["senior_keywords"]

db2 = client["companies"]
cl_Rus = db2["Russell3000_1844"]

keywords = ("ceo", "cfo", "vp", "president", "controller", "treasurer",
            "vicepresident", "chiefaccountingofficer", "chieffinancialofficer",
            "chiefexecutiveofficer", "executivedirector"
            )


def gen_Russell_exp():
    print("Generating Russel exp...")
    Rus_urn_set = set()
    for i in cl_Rus.find():
        Rus_urn_set.add(i.get("companyUrn"))
    print(len(Rus_urn_set))
    for i in cl_aft.find():
        urn = i.get("companyUrn")
        if urn in Rus_urn_set:
            cl.insert(i)


def gen_senior_keywords():
    fuzzy_key = []
    accurate_key = []
    for n, item in enumerate(cl.find()):
        print(n)
        t = item.get("title").strip().replace(" ", "")
        t = t.lower()
        if t in keywords:
            accurate_key.append(t)
        for key in keywords:
            if key in t:
                fuzzy_key.append(t)
                break

    print("----------------------------------------")
    print("Accurate: %d\tFuzzy: %d\tTotal: %d" % (len(accurate_key), len(fuzzy_key), n + 1))

    a = set(accurate_key)
    b = set(fuzzy_key)
    # c = b - a
    f = Counter(fuzzy_key)
    print("\nGenerating...")
    for each in b:
        cl_kw.insert(dict(title=each, frequency=f.get(each)))
    print("Finish")


def update_senior_exp():
    print("Updating senior title sign...")
    for item in cl.find():

        _id = item.get("_id")
        t = item.get("title").strip().replace(" ", "").lower()
        if cl_kw.find_one({"title": t}):
            cl.update_one({"_id":_id},{"$set": {"seniorManagers":True}})

if __name__ == "__main__":
    try:
        gen_Russell_exp()
        # gen_senior_keywords()
        update_senior_exp()
    finally:
        client.close()
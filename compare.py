"""
比较
US_experience.after_Rus3000_exp_old.csv 和 US_experience.after_Rus3000_exp.csv
去掉companyName，nextCompanyTitle，nextCompanyseniorManagers三列后是否相等
"""
import pandas as pda
flag = "99999999"
old_data = pda.read_csv("../data/output/csv/US_experience.after_Rus3000_exp_old.csv").fillna(flag)
data = pda.read_csv("../data/output/csv/US_experience.after_Rus3000_exp.csv")

data = data.drop(columns=["companyName","nextCompanyTitle","nextCompanyseniorManagers"]) # 去掉对应的列
data = data.rename(columns={"dupNextExperience":"dup"}) # 原始的列名为dup，新版本改为了dupNextExperience
columns = old_data.columns 
data = data[columns].fillna(flag) # 将为nan的值填充为一个固定值因为nan!=nan
compare_res = data == old_data
res = "相等" if (~compare_res).sum(0).sum() == 0 else "不相等"
print(res)
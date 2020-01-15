*************************
* Created on 20190207
* Updated on 20160217
*************************
***********************************
* Merging two mobility files
***********************************
cd D:\DataTextual\Small\LinkedIn\LinkedIn_ACC\ACCHires20190216\data\output\csv
insheet using mobility_cnt.csv,clear
save temp.dta,replace


insheet using mobility_senior_cnt.csv,clear
rename incoming senior_incoming
rename outgoing senior_outgoing
rename accnum senior_accnum
merge 1:1 companyurn year month using temp.dta
drop _merge

replace senior_incoming=0 if senior_incoming==.
replace senior_outgoing=0 if senior_outgoing==.
replace senior_accnum=0 if senior_accnum==.

keep if year<=2010

replace month=6.5 if month==0
sort companyurn year month
replace month=0 if month==6.5
save ACC_mobility.dta,replace


erase temp.dta
/*
bysort  companyurn:gen x=_N
tab x

bysort  companyurn year:gen y=_N
tab y
*/

***********************************************************
* Identifying 1718 firms in SHO_FinalforLinkedin_withurn
***********************************************************
use ACC_mobility.dta,clear
keep companyurn
bysort companyurn:keep if _n==1
count
save 1718firms.dta,replace


inshee using D:\DataTextual\Small\LinkedIn\LinkedIn_ACC\ACCHires20190216\data\input\SHO_FinalforLinkedin_withurn.csv,clear
gen seq=_n
count if linkedinurl~="" //2199
count if companyurn~=. //2205

merge m:1 companyurn using 1718firms.dta
tab _merge

count if companyurn~=. //2205
replace companyurn=. if companyurn~=.  & _merge==1 
sort seq
drop _merge linkedinurl seq

count if companyurn~=. //1718
count
save SHO_withurn_final.dta,replace

erase 1718firms.dta








e

****************************
* End of Code
****************************
use ACC_mobility20190208.dta,clear
keep companyurn
bysort companyurn:keep if _n==1
count
save 1718firms.dta,replace


inshee using D:\DataTextual\Small\LinkedIn\LinkedIn_ACC\ACCHires20190204\data\input\SHO_FinalforLinkedin_withurn.csv,clear
count if linkedinurl~=""
count if companyurn~=.

merge m:1 companyurn using 1718firms.dta
tab _merge

count if companyurn~=. //2205
gen check=1 if companyurn~=.  & _merge==1 
tab check

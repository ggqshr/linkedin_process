/*
cd D:\DataTextual\Small\LinkedIn\LinkedIn_ACC\ACCHires20190201
insheet using floating_cnt.csv,clear
keep if month==12
drop if accnum==0

count

tab year //column 2

bysort year: egen number = sum(accnum) 
bysort year:keep if _n==1
keep year number
br
*/


cd D:\DataTextual\Small\LinkedIn\LinkedIn_ACC\ACCHires20190216\data\output\csv
insheet using mobility_cnt.csv,clear
keep if month==12
drop if accnum==0

count

tab year //column 2

bysort year: egen number = sum(accnum) 
bysort year:keep if _n==1
keep year number
br

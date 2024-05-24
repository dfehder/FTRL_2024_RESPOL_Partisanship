/*
CREATED: 03/20/2023
DESC: Collect up all data sets
CREATED BY: D. C. FEHDER
*/


global final "/project/fehder_718/ftrl_2024_respol_data/final/matches/agg_yr/"

* open log
log using lg-3-collect.log, replace

* create base of data with 2014
use "${final}matches_refined_no_multiples_p95_2014"
rename year impute_year
gen year = 2014
save "${final}all_matches_refined_no_multiples_p95", replace

foreach n of numlist 2015/2021 {
	display "`n'"
	use "${final}matches_refined_no_multiples_p95_`n'", clear
	rename year impute_year
	gen year = `n'
	append using "${final}all_matches_refined_no_multiples_p95"
	save "${final}all_matches_refined_no_multiples_p95", replace 
}


keep lalvoterid inventor_id name_last name_first region_code male_flag year impute_year flag address_zip address_latitude address_longitude age dob parties_desc countycode state_code region_type region_title posterior_final2

rename posterior_final2 posterior_final

tab year

save "${final}all_matches_refined_no_multiples_p95_final", replace



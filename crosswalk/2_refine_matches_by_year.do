/*
CREATED: 10/19/2022
DESC: Process matched output
CREATED BY: D. C. FEHDER
*/
*version 16
clear

* open log
log using lg-refine-yr-`1'.log, replace

* get environmental variables
/* run env_vars.do */
global final "/project/fehder_718/ftrl_2024_respol_data/final/matches/agg_yr/"

*cd "${path}"

****************************************************************
*    STARTING NUMBER OF INVENTORS
****************************************************************
import delimited "${final}raw_all_matches_`1'.csv"

* Display total number of inventors up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

****************************************************************
*    CORRECTION FACTOR
****************************************************************


*drop posterior_final

gen rw2_fn = (msum_fn*pgammajm)/((msum_fn*pgammajm)+(usum_fn*pgammaju*3000))
gen rw2_ln = (msum_ln*pgammajm)/((msum_ln*pgammajm)+(usum_ln*pgammaju*3000))


replace rw2_fn = 1 if missing(rw2_fn)
replace rw2_ln = 1 if missing(rw2_ln)

gen posterior_final2 = posterior*rw2_fn*rw2_ln

keep if posterior_final2 >= 0.75

drop _merge

sum posterior_final2

tempfile raw1
save `raw1', replace

* Display total number of inventors up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

* Display total number of voter ids up to this point
by lalvoterid, sort: gen nvals = _n == 1 
count if nvals
drop nvals

****************************************************************
*      	CHOOSE HIGHEST POSTERIOR MATCH(ES) FOR EACH INVENTOR
****************************************************************

* collapse to find max observation

collapse (max) posterior_final2, by(inventor_id)

rename posterior_final2 max_pos

* merge back to main data
merge 1:m inventor_id using `raw1'
drop _merge

* now mark those that are max
gen ismax = 0 
replace ismax = 1 if posterior_final2 == max_pos
sum ismax

keep if ismax == 1

* distribution after restricting to highest posterior match
sum posterior_final2


* generate the data sets for the full (data) with multiples
save "${final}matches_refined_w_multiples_`1'", replace

save `raw1', replace

* Display total number of inventors up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

* Display total number of voter ids up to this point
by lalvoterid, sort: gen nvals = _n == 1 
count if nvals
drop nvals


****************************************************************
*      	REMOVE MATCHES WITH MULTIPLES
****************************************************************


* drop from inventor side
gen mcnt = 1
collapse (sum) mcnt, by(inventor_id)

merge 1:m inventor_id using `raw1'
drop _merge
sum mcnt

keep if mcnt < 2

* Display total number of observations up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

drop mcnt

save `raw1', replace

* Now drop from the L2 id side
gen mcnt = 1
collapse (sum) mcnt, by(lalvoterid)

merge 1:m lalvoterid using `raw1'
drop _merge
sum mcnt

keep if mcnt < 2

* distribution after restricting to singeltons
sum posterior_final2

* Display total number of inventors up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

* Display total number of voter ids up to this point
by lalvoterid, sort: gen nvals = _n == 1 
count if nvals
drop nvals

* generate the data sets for the full (data) with no multiples
save "${final}matches_refined_no_multiples_`1'", replace



****************************************************************
*      	NOW KEEP ONLY THE MATCHES AT OR ABOVE 0.95
****************************************************************
keep if posterior_final2 >= 0.95

* distribution after restricting to above .95
sum posterior_final2

* Display total number of inventors up to this point
by inventor_id, sort: gen nvals = _n == 1 
count if nvals
drop nvals

* Display total number of voter ids up to this point
by lalvoterid, sort: gen nvals = _n == 1 
count if nvals
drop nvals

save "${final}matches_refined_no_multiples_p95_`1'", replace

* close the log
log close



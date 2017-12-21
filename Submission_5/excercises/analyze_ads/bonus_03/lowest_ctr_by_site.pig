
data = LOAD '/dualcore/ad_data[1-2]/part*'
	AS (
		campaign_id:chararray,
             	date:chararray, time:chararray,
             	keyword:chararray, display_site:chararray,
             	placement:chararray, was_clicked:int, cpc:int);

grouped = GROUP data BY display_site;

by_site = FOREACH grouped {
	clicked = FILTER data BY was_clicked == 1;
	num_clicked = COUNT(clicked);
	total = COUNT(data);

  /* Calculate the click-through rate by dividing the 
   * clicked ads in this group by the total number of ads
   * in this group.
   */
	ctr = num_clicked*100.0/total;
	GENERATE group, ctr AS ctr;
}



-- sort the records in ascending order of clickthrough rate
ordered = ORDER by_site BY ctr ASC;

-- show just the first three
top_3 = LIMIT ordered 3;

grouped_KW = GROUP data BY keyword;
by_kw = FOREACH grouped_KW {
	clicked = FILTER data BY was_clicked == 1;
	num_clicked = COUNT(clicked);
	total = COUNT(data);
	ctr = num_clicked*100.0/total;
	GENERATE group, ctr AS ctr;
}

ordered_kw = ORDER by_kw BY ctr DESC;

-- show just the first three
top_3_kw = LIMIT ordered_kw 3;

DUMP top_3;
DUMP top_3_kw;


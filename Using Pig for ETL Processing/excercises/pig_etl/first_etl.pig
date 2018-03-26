data = LOAD '/dualcore/ad_data1.txt' AS (
				keyword: chararray,
				campaign_id: chararray,
				date: chararray,
				time: chararray,
				display_site: chararray,
				was_clicked: int,
				cpc: int,
				country: chararray,
				placement: chararray
				);
just_the_usa = FILTER data BY NOT (country != 'USA');
ordered_data = FOREACH just_the_usa GENERATE
						campaign_id,
						date,
						time,
						UPPER(TRIM(keyword)) AS keyword: chararray,
						display_site,
						placement,
						was_clicked,
						cpc;
--DUMP data
--DUMP just_the_usa;
--DUMP ordered_data;
--STORE ordered_data INTO 'dualcore/ad_data1' USING JsonStorage();
STORE ordered_data INTO '/dualcore/ad_data1';


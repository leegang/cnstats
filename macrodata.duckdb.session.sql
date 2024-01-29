CREATE TEMPORARY TABLE temp_code_existence AS
SELECT code,
       MAX(CASE WHEN time_str > 2000 AND time_str <= 2000 THEN 'Y' ELSE 'N' END) AS has_value_2000,
       MAX(CASE WHEN time_str > 2001 AND time_str <= 2001 THEN 'Y' ELSE 'N' END) AS has_value_2001,
       MAX(CASE WHEN time_str > 2003 AND time_str <= 2004 THEN 'Y' ELSE 'N' END) AS has_value_2004,
       MAX(CASE WHEN time_str > 2005 AND time_str <= 2006 THEN 'Y' ELSE 'N' END) AS has_value_2006,
       MAX(CASE WHEN time_str > 2007 AND time_str <= 2008 THEN 'Y' ELSE 'N' END) AS has_value_2008,
       MAX(CASE WHEN time_str > 2009 AND time_str <= 2010 THEN 'Y' ELSE 'N' END) AS has_value_2010,
       MAX(CASE WHEN time_str > 2011 AND time_str <= 2012 THEN 'Y' ELSE 'N' END) AS has_value_2012,
        MAX(CASE WHEN time_str > 2013 AND time_str <= 2014 THEN 'Y' ELSE 'N' END) AS has_value_2014,
        MAX(CASE WHEN time_str > 2015 AND time_str <= 2016 THEN 'Y' ELSE 'N' END) AS has_value_2016,
        MAX(CASE WHEN time_str > 2017 AND time_str <= 2018 THEN 'Y' ELSE 'N' END) AS has_value_2018,
        MAX(CASE WHEN time_str > 2019 AND time_str <= 2020 THEN 'Y' ELSE 'N' END) AS has_value_2020,
       MAX(CASE WHEN time_str > 2021 AND time_str <= 2022 THEN 'Y' ELSE 'N' END) AS has_value_2022
FROM macrodb_back.main.hgnd_data
GROUP BY code
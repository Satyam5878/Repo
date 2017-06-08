nyse_data = load 'hdfs://localhost:54310/user/pig/NYSE/input/nyse_data.txt' using PigStorage('\t') as (exchange:chararray, ticker:chararray, date:datetime,open:float,high:float,low:float,close:float,volume:int,adjclose:float);

dump nyse_data;

nyse_data_modified = foreach nyse_data generate exchange,ticker,GetYear(date),GetMonth(date),GetDay(date),open,high,low,close,volume,adjclose;

dump nyse_data_modified;

nyse_data_modified2 = foreach nyse_data_modified generate * as(exchange:chararray, ticker:chararray, year:int, month:int, day:int, open:float, high:float,low:float,close:float,volume:int,adjclose:float);

dump nyse_data_modified2;

nyse_data_group = group nyse_data_modified2 by (year,month);

dump nyse_data_group;

nyse_data_avg_volume = foreach nyse_data_group generate (group.year,group.month), AVG(nyse_data_modified2.volume);

dump nyse_data_avg_volume;


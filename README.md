# mapreduce
Using Hadoop's Map Reduce programs, calculate average monthly, yearly, and seasonal (winter, summer, etc) year by year in different stations or areas in dataset and comparing them (programmatically) over the years.

###Download data for over a 25 year period from http://www.ncdc.noaa.gov/cdo-web/datasets.

###How to Run:
1. `hadoop jar ~/climateData.jar edu.uta.cse.ClimateDataRunner <input> <output> <MONTH/SEASON/YEAR> <numMappers> <numReducers>`

e.g.: `hadoop jar ~/climateData.jar edu.uta.cse.ClimateDataRunner /tmp/virginia /tmp/virginia-month MONTH 1 1`

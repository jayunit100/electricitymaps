# This was fun to play with :), however, I didn't have time do do much
# in the way of using lambdas, OO, or function decomposition.

# Haven't done any bigdata APIs lately so i kept it simple
# Scaling the "Code" as quoted in the project wasnt required b/c
# The intervals are naturally self limiting, and my assumption is continous
# ingestion of the data and outputting of it is naturally best done
# in a single process.

# From a "Big data" perspective one might parallelize this by
# 1) Aggregating all entries into separete buckets or channels by timestamp hour
# 2) Periodically running a job on any bucket whose timestamp hour was < 2 hours old
#     this might give an extra hour for old data to make it into the funnel
# 3) run this "job" in parallel per country, i.e. have c jobs reading bucket "hour1day1" in parallel.
# 4) have each "job" output a SUMMARY_COUNTRY.json file to the bucket

# Obviously, this could be done more elegantly by using kafka or beam or spark
# or other modern tools.  They key thing is that you have a data source 
# that is sharded by its hourly timestamp, so that theres a natural "start" and "endpoint"
# for outputting the hourly consumption.

# INPUT
# 
#   The input to this program is a line delimited file
#   of 2 types of json records, ElectrictyExchange and ElectricityProduction.
#   
#   These jsons have a "zone_key", which either has one country or two. 
#   If there are two countries (in case of ElectrictyExchange)
#   they are separated by a "->" delimiter. 
#
#  {
#   "datetime":"2020-07-01T00:00:00+00:00",
#   "kind":"ElectricityExchange",
#   "zone_key":"DK-DK1->NO-NO2",
#   "data":{
#       "source": "api.energidataservice.dk", 
#       "netFlow": -331.69, 
#       "schemaVersion": 3, 
#       "sortedCountryCodes": "DK-DK1->NO-NO2"}
#   }
#  }
#
# { 
#  "datetime":"2020-07-01T00:00:00+00:00",
#  "kind":"ElectricityProduction",
#  "zone_key":"DK-DK1",
#   "data":{
#        "source": "entsoe.eu", 
#        "storage": 
#              {"hydro": null}, 
#              
#              "production": {
#                "gas": 79.0, 
#                "oil": 12.0, 
#                "coal": 264.0, 
#
# The main logic of this program is to generate aggregated output
# as described below which calculates the total consumption, based on
# the equation c = P + (I - E)

# OUTPUT
#
# The output of this program is one json record, for every "day-hour".  This
# record will have the imported, exported, produced, and consumption data for
# each country for which *any* data is available, during the said hour.
# 
# If no data is available for a country, it will not be in that hourly json.
# If any data is missing for a country (exported, imported, produced), that 
# country will have a "zero" value for this field.  Note that there may be 
# more sophisticated ways to interpolate this value from past data, but we 
# dont attempt such feats of magic in this simple prototype.
# 
# "DE": {
#  "E": 2089.7000000000003,
#  "I": 0, 
#  "prod": 0,
#  "consumption": -2089.7000000000003,
#  "meta": {
#   "hour": "00",
#   "day": "01"
#  }
# }, 
# "DE": {
#  "E": 2089.7000000000003,
#  "I": 0, 
# ...

import json

### Dummy initialization values for our first "day/hour" record.
### We'll use these as global variables, and turn them over every time we see a new
### Hourly timestamp.
### This program is stateless, and thus it will continually delete these
### every time the day-hour changes... We' assume the outputs of this program
### can be fed into a persistant place if needed. 
DAY = "0000"
HOUR = "0000"
COUNTRIES = {}
HOURS = 0

# This function process process a new line item, assuming all line items are fed to this function in
# increasing timestamp order.  If a "Filter" is sent, i.e. "DE", then
# we'll only print stats for the Country in the filter.
def process(line, filter):
    global DAY
    global HOUR
    global COUNTRIES    
    global HOURS
    r = json.loads(line)
    
    # Parse the day and hour from a string such as this
    # "datetime":"2020-07-01T00:00:00+00:00"
    day=r["datetime"].split("-")[2].split("T")[0]
    hour= r["datetime"].split("-")[2].split(":")[0].split("T")[1]

    # def clear(): 
    #   this runs:
    #   - on the first record
    #   - any time the incoming record is a "new" hour
    if day != DAY or hour != HOUR:
        HOURS += 1
        
        for key in COUNTRIES:
            country=COUNTRIES[key]
            # Add zero's for missing values...
            for v in ["prod", "I", "E"]:
                if v not in country:
                    country[v]=0
            # Calculate consumption
            COUNTRIES[key]["consumption"]= country["prod"] + (country["I"] - country["E"])
            COUNTRIES[key]["meta"]={}
            COUNTRIES[key]["meta"]["hour"]=HOUR
            COUNTRIES[key]["meta"]["day"]=DAY
            
        if filter is not None and filter in COUNTRIES:
            print( json.dumps(COUNTRIES[filter], indent=" "), "\n" )
        else:
            print( json.dumps(COUNTRIES, indent=" "), "\n" )

        ### Now, we'll clear this data... the next hour has begun.        
        DAY = day
        HOUR = hour
        COUNTRIES = {}

    # now parse the record and integrate it into our global datamodel
    # def parse():
    if r["kind"] == "ElectricityExchange":
        cFrom = r["zone_key"].split("->")[0]
        cTo = r["zone_key"].split("->")[1]
        netFlow = r["data"]["netFlow"]
        
        # print("Parsed: ", cFrom, cTo, "flow =>", netFlow)


        for c in [cFrom, cTo]:
            for v in ["E","I","prod"]:
                # add countries if not existing
                if c not in COUNTRIES:
                    COUNTRIES[c] = {}
                if v not in COUNTRIES[c]:
                    COUNTRIES[c][v] = 0

        # Exported power counts as "unconsumed", i.e. negative power consumption
        COUNTRIES[cFrom]["E"] -= netFlow
        # Imported power counts as "consumed" power, i.e. positive power consumption
        COUNTRIES[cTo]["I"] += netFlow
    
    elif r["kind"] == "ElectricityProduction":
        cFrom = r["zone_key"]
        if cFrom not in COUNTRIES:
            COUNTRIES[cFrom] = {}
        if "prod" not in COUNTRIES[cFrom]:
            COUNTRIES[cFrom]["prod"] = 0
        
        prodmap=r["data"]["production"]
        prodmap = {k: v for k, v in prodmap.items() if v is not None}

        # print(prodmap.values())
        prod_total = sum(prodmap.values())
        # print("production total", prodmap, "was", prod_total)
        COUNTRIES[cFrom]["prod"] += prod_total

    else:
        print("failed record EXITING")
        print(r)
        exit()

### A Simple "unit test" you can run from main
def main1():
    jsonn =  '''{
            "datetime":"2020-07-01T00:00:00+00:00",
            "kind":"ElectricityExchange",
            "zone_key":"DK-DK1->NO-NO2",
            "data":{
                "source": "api.energidataservice.dk", 
                "netFlow": -331.69, 
                "schemaVersion": 3, 
                "sortedCountryCodes": "DK-DK1->NO-NO2"
            }
    }'''
    process(jsonn, None)

# A full test of the 30 day dataset
def main2():
    global HOURS
    global COUNTRIES
    f = open('30_days.jsonl', 'r')
    Lines = f.readlines()
    count = 0
    # Strips the newline character
    for line in Lines:
        count += 1
        process(line, None)

    print(COUNTRIES) 
    print(HOURS)


if __name__ == "__main__":
    main2()
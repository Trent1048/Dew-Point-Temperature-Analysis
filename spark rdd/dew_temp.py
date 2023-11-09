from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Dew Temp").setMaster("local")
sc = SparkContext(conf=conf)

temperatureInput = sc.textFile("./normal_hly_sample_temperature.csv")

def mapTemperatureLine(line: str):
    tokens = line.split(",")
    if tokens[0] == "STATION":
        return ("NULL", "0, 0, 0")
    
    date = tokens[5].split(" ")[0]
    normalTemp = tokens[6]
    dewTemp = tokens[7]
    temperatures = normalTemp + ", " + dewTemp

    return (date, temperatures + ", 1")

def reduceTemperatureAverage(tempVal1, tempVal2):
    normalTemp1, dewTemp1, count1 = tempVal1.split(", ")
    normalTemp2, dewTemp2, count2 = tempVal2.split(", ")

    resultNormalTemp = str(float(normalTemp1) + float(normalTemp2))
    resultDewTemp = str(float(dewTemp1) + float(dewTemp2))
    resultCount = str(int(count1) + int(count2))

    return resultNormalTemp + ", " + resultDewTemp + ", " + resultCount

#def collectTemperatureAverage(tempVal1, tempVal2):
#    avgNormalTemp1, avgDewTemp1, count1 = tempVal1.split(", ")
#    avgNormalTemp2, avgDewTemp2, count2 = tempVal2.split(", ")

pairs = temperatureInput.map(mapTemperatureLine)
counts = pairs.reduceByKey(reduceTemperatureAverage)
#counts = pairs.aggregateByKey("0, 0, 0", reduceTemperatureAverage, collectTemperatureAverage)

counts.sortByKey()
print(counts.collect())
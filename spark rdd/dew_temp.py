from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Dew Temp").setMaster("local")
sc = SparkContext(conf=conf)

temperatureInput = sc.textFile("./normal_hly_sample_temperature.csv")

def mapTemperatureLine(line: str):
    tokens = line.split(",")
    if tokens[0] == "STATION":
        return ("NULL", (0, 0, 1))
    
    date = tokens[5].split(" ")[0]
    normalTemp = float(tokens[6])
    dewTemp = float(tokens[7])

    return (date, (normalTemp, dewTemp, 1))

def reduceTemperatureAverage(tempVal1, tempVal2):
    normalTemp1, dewTemp1, count1 = tempVal1
    normalTemp2, dewTemp2, count2 = tempVal2

    resultNormalTemp = normalTemp1 + normalTemp2
    resultDewTemp = dewTemp1 + dewTemp2
    resultCount = count1 + count2

    return (resultNormalTemp, resultDewTemp, resultCount)

def combineTemperatureAverage(tempVal):
    normalTemp, dewTemp, count = tempVal

    return (normalTemp / count, dewTemp / count)

pairs = temperatureInput.map(mapTemperatureLine)
counts = pairs.reduceByKey(reduceTemperatureAverage)
averages = counts.mapValues(combineTemperatureAverage)

averages.sortByKey()

with open("./spark rdd/output.txt", "w") as output:
    for data in averages.collect():
        date, (avgNormalTemp, avgDewTemp) = data
        if date == "NULL":
            continue
        output.write(date + "\t" + str(avgNormalTemp) + ", " + str(avgDewTemp) + "\n")
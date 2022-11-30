ipDict = {}
day = 0

while day < 31:
    day += 1
    filename = "dns-names.l7.202110{day}.txt".format(day=str(day).zfill(2))
    with open("./data/dns_data/" + filename, "r") as inputFile:
        invalidLines = 0
        validLines = 0
        for line in inputFile:
            splitLine = line.split()
            if len(splitLine) != 3:
                invalidLines += 1
                continue
            domainFragments = splitLine[2].split('.')
            if len(domainFragments) < 3 or domainFragments[0] == 'www':
                invalidLines += 1
                continue
            validLines += 1
            ipDict[splitLine[1]] = splitLine[2]
#        with open("data/log.txt", "a") as outputFile:
#            outputFile.write(filename + ' - ' + str(invalidLines) + ' - ' + str(validLines) + '\n')
        print("Done with", filename)

fileNr = 0
index = 0
sortedDict = sorted(ipDict)
while index < len(sortedDict):
    fileNr += 1
    if fileNr > 15:
        break
    with open("./data/aggregated/aggregatedDnsNames{fileNr}.txt".format(fileNr=fileNr), "w") as outputFile:
        while True:
            outputFile.write(sortedDict[index] + ' ' + ipDict[sortedDict[index]] + '\n')
            index += 1
            if index % 6000000 == 0 or index >= len(sortedDict):
                break

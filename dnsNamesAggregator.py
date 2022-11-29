import re

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
            splitDomain = re.split('[-.\\d]', splitLine[2])
            if len(splitDomain) < 3 or splitDomain[0] == 'www':
                invalidLines += 1
                continue
            validLines += 1
            ipDict[splitLine[1]] = splitLine[2]
        with open("data/log.txt", "a") as outputFile:
            outputFile.write(filename + ' - ' + str(invalidLines) + ' - ' + str(validLines) + '\n')
        print("Done with", filename)

with open("data/aggregatedDnsNames.txt", "w") as outputFile:
    for ip in ipDict:
        outputFile.write(ip + ' ' + ipDict[ip] + '\n')

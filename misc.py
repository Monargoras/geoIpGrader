import json
import time
from DatabaseIO import DatabaseIO


def addPopulationDnsHint(hint: list):
    with open("./data/cities500.txt", encoding='utf-8') as inputTxtFile:
        length = 199301
        count = 0
        for line in inputTxtFile:
            count += 1
            lineArray = line.strip().split('\t')
            cityName = lineArray[2]
            # cityLat = lineArray[4]
            # cityLng = lineArray[5]
            countryCode = lineArray[8]
            cityPopulation = lineArray[14]
            if (cityName.upper() in hint[6] or hint[6] in cityName.upper()) and countryCode == hint[8]:
                if cityPopulation == '':
                    continue
                if cityPopulation == '0':
                    continue
                print(count, '/', length, '-----', cityName, countryCode, int(cityPopulation))
                while not dbio.updateDnsHint(rowId=hint[0], population=int(cityPopulation)):
                    print('Failed to add population to database. Retrying...')
                    time.sleep(1)


def insertDomainRegexes():
    with open("./data/202103-midar-iff.geo-re.json", encoding='utf-8') as inputJsonFile:
        data = []
        for line in inputJsonFile:
            lineJson = json.loads(line)
            domain = lineJson['domain']
            regex = lineJson['re']
            score = lineJson['score']
            print(domain)
            print(regex)
            print(score['class'], score['score'], score['atp'])
            data.append((domain, json.dumps(regex), score['class'], score['score'], score['atp']))
        print('Inserting', len(data), 'regexes...')
        dbio.insertRegexes(data)


if __name__ == '__main__':
    dbio = DatabaseIO()
    allHints = dbio.getDnsHints()
    for geoHint in allHints:
        addPopulationDnsHint(geoHint)

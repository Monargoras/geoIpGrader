import json
import time
from DatabaseIO import DatabaseIO


def addPopulationDnsHints():
    with open("./data/cities500.txt", encoding='utf-8') as inputTxtFile:
        dbio = DatabaseIO()
        length = 199301
        count = 0
        failed = 0
        for line in inputTxtFile:
            count += 1
            lineArray = line.strip().split('\t')
            cityName = lineArray[2]
            # cityLat = lineArray[4]
            # cityLng = lineArray[5]
            countryCode = lineArray[8]
            cityPopulation = lineArray[14]
            if cityPopulation == '':
                failed += 1
                continue
            if cityPopulation == '0':
                failed += 1
                continue
            print(count, '/', length, '-----', cityName, countryCode, int(cityPopulation))
            while not dbio.addPopulation(countryCode=countryCode, cityName=cityName, population=int(cityPopulation)):
                print('Failed to add population to database. Retrying...')
                time.sleep(1)

        print('Failed:', failed)


def insertDomainRegexes():
    with open("./data/202103-midar-iff.geo-re.json", encoding='utf-8') as inputJsonFile:
        dbio = DatabaseIO()
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
    insertDomainRegexes()

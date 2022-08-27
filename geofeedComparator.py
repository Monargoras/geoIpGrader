import csv
from DatabaseIO import DatabaseIO
from Databases import Databases
from geopy import distance


def compareGeofeeds():

    consistentMaxMind = 0
    maxMindComparisons = 0
    consistentRipeIpmap = 0
    ripeIpmapComparisons = 0
    totalComparisons = 0

    distanceBuffer = 10

    file = csv.reader(open('geofeedsIPv4.csv'), delimiter=',')
    next(file)

    for line in file:
        ipStartLong = int(line[1])
        ipEndLong = int(line[2])
        ipsAreEqual = ipStartLong == ipEndLong
        latitude = line[6]
        longitude = line[7]
        dbio = DatabaseIO()
        startLocations = dbio.getIpLocationMaxMindRipe(ipStartLong)
        endLocations = dbio.getIpLocationMaxMindRipe(ipEndLong)
        for d in [e.name for e in [Databases.maxPayed, Databases.ripeIpmap]]:
            # add ip check (ip_start_long <= ip_from_db_we_are_evaluating (:= evalIP))
            if startLocations[d][0] is None or not startLocations[d][0] <= ipStartLong:
                startLocations[d][2] = None
                startLocations[d][3] = None
            if not ipsAreEqual and (endLocations[d][0] is None or not endLocations[d][0] <= ipEndLong):
                endLocations[d][2] = None
                endLocations[d][3] = None

        for d in [e.name for e in [Databases.maxPayed, Databases.ripeIpmap]]:
            if startLocations[d][2] is not None and startLocations[d][3] is not None:
                if distance.distance((latitude, longitude), (startLocations[d][2], startLocations[d][3])).km <= distanceBuffer:
                    if d == Databases.maxPayed.name:
                        consistentMaxMind += 1
                    else:
                        consistentRipeIpmap += 1
                if d == Databases.maxPayed.name:
                    maxMindComparisons += 1
                else:
                    ripeIpmapComparisons += 1
            if not ipsAreEqual and endLocations[d][2] is not None and endLocations[d][3] is not None:
                if distance.distance((latitude, longitude), (endLocations[d][2], endLocations[d][3])).km <= distanceBuffer:
                    if d == Databases.maxPayed.name:
                        consistentMaxMind += 1
                    else:
                        consistentRipeIpmap += 1
                if d == Databases.maxPayed.name:
                    maxMindComparisons += 1
                else:
                    ripeIpmapComparisons += 1
        totalComparisons += 1 if ipsAreEqual else 2
        print(totalComparisons, 'IPs')

    print('Distance buffer:', distanceBuffer, 'km')
    print('Consistent Comparisons Maxmind Payed:', consistentMaxMind)
    print('Total Comparisons Maxmind Payed:', maxMindComparisons)
    print('Consistent Comparisons RipeIPMAP:', consistentRipeIpmap)
    print('Total Comparisons RipeIPMAP:', ripeIpmapComparisons)
    print('Total Comparisons:', totalComparisons)


if __name__ == '__main__':
    compareGeofeeds()

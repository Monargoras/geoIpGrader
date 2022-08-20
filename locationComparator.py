from DatabaseIO import DatabaseIO
from geopy import distance


def comparePeeringDCM():
    dbio = DatabaseIO()
    peeringLoc = dbio.getPeeringLocations()
    index = 0
    totalEqualLocations = 0
    totalUniqueLocations = 0
    while index < len(peeringLoc):
        curAsn = peeringLoc[index][1]
        curAsnList = []
        while index < len(peeringLoc) and peeringLoc[index][1] == curAsn:
            curAsnList.append(peeringLoc[index])
            index += 1
        dcmAsn = dbio.getDCMLocations(curAsn)
        dcmCoords = [(lat, long) for (a, b, lat, long) in dcmAsn]
        foundEqualLoc = False
        for location in curAsnList:
            for dcmLoc in dcmCoords:
                if distance.distance((location[2], location[3]), dcmLoc).km < 50:
                    foundEqualLoc = True
                    break
            if foundEqualLoc:
                totalEqualLocations += 1
            else:
                totalUniqueLocations += 1
        print('Peering vs DCM', '-----', 'Equal:', totalEqualLocations, '-----', 'Unique', totalUniqueLocations)


def compareDCMPeering():
    dbio = DatabaseIO()
    dcmLoc = dbio.getDCMLocations()
    index = 0
    totalEqualLocations = 0
    totalUniqueLocations = 0
    while index < len(dcmLoc):
        curAsn = dcmLoc[index][1]
        curAsnList = []
        while index < len(dcmLoc) and dcmLoc[index][1] == curAsn:
            curAsnList.append(dcmLoc[index])
            index += 1
        peeringAsn = dbio.getPeeringLocations(curAsn)
        peeringCoords = [(lat, long) for (a, b, lat, long) in peeringAsn]
        foundEqualLoc = False
        for location in curAsnList:
            for peeringLoc in peeringCoords:
                if distance.distance((location[2], location[3]), peeringLoc).km < 50:
                    foundEqualLoc = True
                    break
            if foundEqualLoc:
                totalEqualLocations += 1
            else:
                totalUniqueLocations += 1
        print('DCM vs Peering', '-----', 'Equal:', totalEqualLocations, '-----', 'Unique', totalUniqueLocations)


if __name__ == '__main__':
    comparePeeringDCM()
    compareDCMPeering()

import math
import logging
from DatabaseIO import DatabaseIO
from Databases import Databases
from geopy import distance


logging.getLogger().setLevel(logging.INFO)

evalDB = Databases.maxFree.name

dbio = DatabaseIO()

while True:
    done, total = dbio.getProgress(evalDB)
    logging.info(evalDB + ' ----- ' + str(done) + ' / ' + str(total))
    if done == total:
        break

    ipList = dbio.getIps(evalDB)

    consistentIPS = 0
    totalIPS = 0
    # blockNumber = 0
    for ipStartLong, ipEndLong, latitude, longitude in ipList:
        # blockNumber += 1
        ip = ipEndLong
        recordDict = dbio.getIpLocations(ip)
        evalLocation = (recordDict[evalDB][2], recordDict[evalDB][3])

        noneLocations = 0
        for d in [e.name for e in Databases]:
            if d == evalDB:
                continue
            # add ip check (ip_start_long <= ip_from_db_we_are_evaluating (:= evalIP))
            if recordDict[d][0] is None or not recordDict[d][0] <= ip:
                recordDict[d][2] = None
                recordDict[d][3] = None
                noneLocations += 1

        locations = {d: (recordDict[d][2], recordDict[d][3]) for d in [e.name for e in Databases]}
        # check distance between evalIP and rest
        # distance < 50 km -> counts as same location
        count = 1
        for d in [e.name for e in Databases]:
            if d == evalDB:
                continue
            if distance.distance(evalLocation, (recordDict[d][2], recordDict[d][3])).km <= 50:
                count += 1

        # majority vote on location
        # if evalDB points to this location count as consistent entry
        if count > math.ceil((5-noneLocations)/2):
            consistentIPS += 1
        totalIPS += 1
    # print('Consistent:', consistentIPS, '----- Total:', totalIPS, '----- Invalid Locations:', noneLocations, '-----', blockNumber, '/', len(ipList))

    # safe evalStep in db
    dbio.safeEvaluation(evalDB, consistentIPS, totalIPS)

    dbio.markIpsDone(evalDB, [[ipEnd] for (ipStart, ipEnd, latitude, longitude) in ipList])

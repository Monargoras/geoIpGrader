import grader
import logging
from Databases import Databases
from DatabaseIO import DatabaseIO


logging.getLogger().setLevel(logging.INFO)
dbio = DatabaseIO()


def printEvaluation():
    res = {}
    for d in [e.name for e in Databases]:
        consistent, total = dbio.getEvaluation(d)
        res[d] = [consistent, total, round((consistent / total), 4)]
    x = 0
    for d in [e.name for e in Databases]:
        x += (1/(1-res[d][2])) if (1-res[d][2]) > 0 else 10000
    for d in [e.name for e in Databases]:
        # voting shares ausrechnen vs_i = (1/(1-DCR_i))/sum of for all dbs (1/(1-DCR_i))
        x_d = (1/(1-res[d][2])) if (1-res[d][2]) > 0 else 10000
        print(d, '-----', res[d][0], '-----', res[d][1], '-----', 'DCR:', str(round(res[d][2] * 100, 2)) + '%',
              '-----', 'Voting share:', str(round((x_d/x) * 100, 2)) + '%')


if __name__ == '__main__':
    for db in [e.name for e in Databases]:
        grader.gradeGeoIp(db)
    printEvaluation()

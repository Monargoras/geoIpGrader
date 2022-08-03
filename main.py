import logging
from DatabaseIO import DatabaseIO
from Databases import Databases


logging.getLogger().setLevel(logging.INFO)
dbio = DatabaseIO()


def printEValuation():
    for d in [e.name for e in Databases]:
        consistent, total = dbio.getEvaluation(d)
        print(d, '-----', consistent, '-----', total, '-----', str(round((consistent / total) * 100, 2)) + '%')

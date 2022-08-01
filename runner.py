import multiprocessing
import grader
from Databases import Databases


def startProcess(database: str):
    multiprocessing.Process(target=grader.gradeGeoIp, args=(database, )).start()


if __name__ == '__main__':
    for d in [e.name for e in Databases]:
        startProcess(d)

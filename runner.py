import grader
from Databases import Databases


if __name__ == '__main__':
    for d in [e.name for e in Databases]:
        grader.gradeGeoIp(d)

from enum import Enum


# additionally use ripeIPMAP API
class Databases(Enum):
    dbip = 'dbip'
    ip2loc = 'ip2location'
    maxFree = 'maxmindFree'
    maxPayed = 'maxmindPayed'
    ripeIpmap = 'ripeIpmap'

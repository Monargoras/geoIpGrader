from Databases import Databases
import math
from DatabaseIO import DatabaseIO
import os
import sys
import utils

ip = utils.ipToLong('104.44.29.68')
apiResult = utils.ripeIpmapRequest(utils.longToIp(ip))
print([ip, ip, apiResult[0], apiResult[1]])
print(utils.ripeIpmapRequest('104.44.29.68'))

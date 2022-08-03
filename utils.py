import ipaddress
import requests
from typing import Tuple


def validateIpv4(ip: str) -> bool:
    return True if type(ipaddress.ip_address(ip)) is ipaddress.IPv4Address else False


def getFirstAndLastIpFromCidr(block: str) -> Tuple[str, str]:
    if type(ipaddress.ip_network(block)) is ipaddress.IPv4Network:
        network = ipaddress.IPv4Network(block)
    else:
        network = ipaddress.IPv6Network(block)
    return str(network[0]), str(network[-1])


def ipToLong(ip: str) -> int:
    return int(ipaddress.ip_address(ip))


def longToIp(ip: int) -> str:
    return str(ipaddress.ip_address(ip))


def ripeIpmapRequest(ip: str) -> tuple | None:
    response = requests.get('https://ipmap-api.ripe.net/v1/locate/{ip}/best'.format(ip=ip))
    if not response.ok:
        return None
    location = response.json()['location']
    if location:
        return location['latitude'], location['longitude']
    else:
        return None

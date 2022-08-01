from typing import Dict, Union
import mysql.connector

from utils import ipToLong, validateIpv4, getFirstAndLastIpFromCidr
from Databases import Databases


class DatabaseIO:
    cnx = None

    def establishConnection(self) -> None:
        try:
            self.cnx = mysql.connector.connect(user='geoIp', password='geoIp',
                                               host='127.0.0.1', database='geo_Ip')
        except mysql.connector.Error as error:
            print('DatabaseIO.establishConnection', error)

    def getIps(self, dbName: str) -> list | None:
        cursor = None
        result = []
        try:
            self.establishConnection()

            sqlDbIp = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM dbip_free_clean WHERE done = FALSE limit 10000"""
            sqlIp2Location = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM ip2location_free_clean WHERE done = FALSE limit 10000"""
            sqlMaxmindFree = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM maxmind_free_clean WHERE done = FALSE limit 10000"""
            sqlMaxmindPayed = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM maxmind_payed_clean WHERE done = FALSE limit 10000"""
            sqlRipeIpmap = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM ripeipmap_clean WHERE done = FALSE limit 10000"""

            cursor = self.cnx.cursor()

            match dbName:
                case Databases.dbip.name:
                    cursor.execute(sqlDbIp)
                case Databases.ip2loc.name:
                    cursor.execute(sqlIp2Location)
                case Databases.maxFree.name:
                    cursor.execute(sqlMaxmindFree)
                case Databases.maxPayed.name:
                    cursor.execute(sqlMaxmindPayed)
                case Databases.ripeIpmap.name:
                    cursor.execute(sqlRipeIpmap)

            result = cursor.fetchall()

            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return None
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return result

    def getIpLocations(self, ip: int) -> Dict[str, Union[list, None]] | None:
        cursor = None
        result = {}
        try:
            self.establishConnection()

            sqlDbIp = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM dbip_free_clean
                         WHERE ip_end_long >= {ipEnd} ORDER BY ip_end_long limit 1""".format(ipEnd=ip)
            sqlIp2Location = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM ip2location_free_clean
                         WHERE ip_end_long >= {ipEnd} ORDER BY ip_end_long limit 1""".format(ipEnd=ip)
            sqlMaxmindFree = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM maxmind_free_clean
                         WHERE ip_end_long >= {ipEnd} ORDER BY ip_end_long limit 1""".format(ipEnd=ip)
            sqlMaxmindPayed = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM maxmind_payed_clean
                         WHERE ip_end_long >= {ipEnd} ORDER BY ip_end_long limit 1""".format(ipEnd=ip)
            sqlRipeIpmap = """SELECT ip_start_long, ip_end_long, latitude, longitude FROM ripeipmap_clean
                         WHERE ip_end_long >= {ipEnd} ORDER BY ip_end_long limit 1""".format(ipEnd=ip)

            cursor = self.cnx.cursor()

            cursor.execute(sqlDbIp)
            result[Databases.dbip.name] = list(cursor.fetchone())

            cursor.execute(sqlIp2Location)
            result[Databases.ip2loc.name] = list(cursor.fetchone())

            cursor.execute(sqlMaxmindFree)
            result[Databases.maxFree.name] = list(cursor.fetchone())

            cursor.execute(sqlMaxmindPayed)
            result[Databases.maxPayed.name] = list(cursor.fetchone())

            cursor.execute(sqlRipeIpmap)
            result[Databases.ripeIpmap.name] = list(cursor.fetchone())

            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return None
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            for d in [e.name for e in Databases]:
                if d not in result.keys():
                    result[d] = [None, None, None, None]
            return result

    def safeEvaluation(self, dbName: str, consistentEntries: int, totalEntries: int) -> bool:
        cursor = None

        try:
            self.establishConnection()
            sql = """INSERT INTO evaluation_results (`database`, consistent_entries, total_evaluated) VALUES (%s, %s, %s)"""

            cursor = self.cnx.cursor()
            cursor.execute(sql, (dbName, consistentEntries, totalEntries))
            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return False
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return True

    def markIpsDone(self, dbName: str, ipList: list) -> bool:
        cursor = None
        try:
            self.establishConnection()

            sqlDbIp = """UPDATE dbip_free_clean SET done = TRUE WHERE ip_end_long = %s"""
            sqlIp2Location = """UPDATE ip2location_free_clean SET done = TRUE WHERE ip_end_long = %s"""
            sqlMaxmindFree = """UPDATE maxmind_free_clean SET done = TRUE WHERE ip_end_long = %s"""
            sqlMaxmindPayed = """UPDATE maxmind_payed_clean SET done = TRUE WHERE ip_end_long = %s"""
            sqlRipeIpmap = """UPDATE ripeipmap_clean SET done = TRUE WHERE ip_end_long = %s"""

            cursor = self.cnx.cursor()

            match dbName:
                case Databases.dbip.name:
                    cursor.executemany(sqlDbIp, ipList)
                case Databases.ip2loc.name:
                    cursor.executemany(sqlIp2Location, ipList)
                case Databases.maxFree.name:
                    cursor.executemany(sqlMaxmindFree, ipList)
                case Databases.maxPayed.name:
                    cursor.executemany(sqlMaxmindPayed, ipList)
                case Databases.ripeIpmap.name:
                    cursor.executemany(sqlRipeIpmap, ipList)

            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return False
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return True

    def getProgress(self, dbName: str) -> list | None:
        cursor = None
        result = []
        try:
            self.establishConnection()

            sqlDbIp = """SELECT COUNT(*) FROM dbip_free_clean WHERE done = TRUE"""
            sqlIp2Location = """SELECT COUNT(*) FROM ip2location_free_clean WHERE done = TRUE"""
            sqlMaxmindFree = """SELECT COUNT(*) FROM maxmind_free_clean WHERE done = TRUE"""
            sqlMaxmindPayed = """SELECT COUNT(*) FROM maxmind_payed_clean WHERE done = TRUE"""
            sqlRipeIpmap = """SELECT COUNT(*) FROM ripeipmap_clean WHERE done = TRUE"""

            cursor = self.cnx.cursor()

            match dbName:
                case Databases.dbip.name:
                    cursor.execute(sqlDbIp)
                case Databases.ip2loc.name:
                    cursor.execute(sqlIp2Location)
                case Databases.maxFree.name:
                    cursor.execute(sqlMaxmindFree)
                case Databases.maxPayed.name:
                    cursor.execute(sqlMaxmindPayed)
                case Databases.ripeIpmap.name:
                    cursor.execute(sqlRipeIpmap)

            result.append(cursor.fetchone()[0])

            sqlDbIp = """SELECT COUNT(*) FROM dbip_free_clean"""
            sqlIp2Location = """SELECT COUNT(*) FROM ip2location_free_clean"""
            sqlMaxmindFree = """SELECT COUNT(*) FROM maxmind_free_clean"""
            sqlMaxmindPayed = """SELECT COUNT(*) FROM maxmind_payed_clean"""
            sqlRipeIpmap = """SELECT COUNT(*) FROM ripeipmap_clean"""

            match dbName:
                case Databases.dbip.name:
                    cursor.execute(sqlDbIp)
                case Databases.ip2loc.name:
                    cursor.execute(sqlIp2Location)
                case Databases.maxFree.name:
                    cursor.execute(sqlMaxmindFree)
                case Databases.maxPayed.name:
                    cursor.execute(sqlMaxmindPayed)
                case Databases.ripeIpmap.name:
                    cursor.execute(sqlRipeIpmap)

            result.append(cursor.fetchone()[0])

            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return None
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return result

    def normalizeRipeIpmap(self) -> bool:
        cursor = None
        result = []
        try:
            self.establishConnection()

            sql = """SELECT ip, latitude, longitude FROM ripeipmap WHERE ip_end_long = 0 LIMIT 10000"""

            cursor = self.cnx.cursor()
            cursor.execute(sql)

            result = cursor.fetchall()

            self.cnx.commit()

            if len(result) == 0:
                return False

        except mysql.connector.Error as error:
            print(error)
            return False
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
                if self.updateRipeIpmap(result):
                    self.removeFinishedRecords(result)
                    return True
                return False

    def updateRipeIpmap(self, records: list) -> bool:
        cursor = None
        data = []
        for row in records:
            ip_start, ip_end = getFirstAndLastIpFromCidr(row[0])
            if not validateIpv4(ip_end):
                continue
            ip_start_long = ipToLong(ip_start)
            ip_end_long = ipToLong(ip_end)
            data.append((ip_start_long, ip_end_long, row[1], row[2]))

        try:
            self.establishConnection()
            sql = """INSERT INTO ripeipmap_clean (ip_start_long, ip_end_long, latitude, longitude) VALUES (%s, %s, %s, %s)"""

            cursor = self.cnx.cursor()
            cursor.executemany(sql, data)
            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return False
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return True

    def removeFinishedRecords(self, records: list) -> bool:
        cursor = None
        try:
            self.establishConnection()

            sql = """INSERT INTO ripeipmap (ip, latitude, longitude) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE ip_end_long = NULL"""

            cursor = self.cnx.cursor()
            cursor.executemany(sql, records)
            self.cnx.commit()

        except mysql.connector.Error as error:
            print(error)
            return False
        finally:
            if self.cnx.is_connected():
                self.cnx.close()
            if cursor is not None:
                cursor.close()
            return True

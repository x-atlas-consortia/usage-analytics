import csv
import ipaddress

class IP2Geo:
    #cache IPs already seen when were doing a bunch of these

    invalid_geo = {'country_code': 'INVALID', 'country_name': 'INVALID', 'region_name': 'INVALID', 'city_name': 'INVALID', 'zip_code': 'INVALID'}
    unknown_geo = {'country_code': 'UNKNOWN', 'country_name': 'UNKNOWN', 'region_name': 'UNKNOWN', 'city_name': 'UNKNOWN', 'zip_code': 'UNKNOWN'}
    unknown_multiple_geo = {'country_code': 'MULTIPLE', 'country_name': 'MULTIPLE', 'region_name': 'MULTIPLE', 'city_name': 'MULTIPLE', 'zip_code': 'MULTIPLE'}
    pittsburgh_geo = {'country_code': 'US', 'country_name': 'United States of America', 'region_name': 'Pennsylvania', 'city_name': 'Pittsburgh', 'zip_code': '15213'}
    local_ip_network = ipaddress.ip_network("10.0.0.0/8")

    def __init__(self, ip_info_filename):
        self.geo_info = []
        with open(ip_info_filename, newline='') as ip_info_file:
            reader = csv.DictReader(ip_info_file, delimiter='\t')
            for row in reader:
                if 'ip_from' not in row or 'ip_to' not in row:
                    continue
                self.geo_info.append({
                    'ip_from': int(row['ip_from'])
                    , 'ip_to': int(row['ip_to'])
                    , 'country_code': row.get('country_code')
                    , 'country_name': row.get('country_name')
                    , 'region_name': row.get('region_name')
                    , 'city_name': row.get('city_name')
                    , 'zip_code': row.get('zip_code')
                })
        self.ip_cache = {}

    def is_valid_ip(self, ip_str):
        try:
            ipaddress.ip_address(ip_str)
            return True
        except ValueError:
            return False


    #return a dict with geolocation info for an IP address
    #input ip address as a string like "125.346.789.121"
    #this only works for IPV4 addresses
    def get_ip_geo_info(self, ip_addr):
        if ip_addr is None or not isinstance(ip_addr, str) or ip_addr.strip() == '':
            return IP2Geo.invalid_geo

        ip_addrL = ip_addr.strip()
        if ip_addrL in self.ip_cache:
            return self.ip_cache[ip_addrL]

        if not self.is_valid_ip(ip_addrL):
            return(IP2Geo.invalid_geo)

        ip = ipaddress.ip_address(ip_addrL)
        if ip in IP2Geo.local_ip_network:
            rval = IP2Geo.pittsburgh_geo
        else:
            ip_int = int(ip)
            # Same three-way outcome as the original pandas boolean-mask filter (0 matches,
            # exactly 1, or 2+) -- a plain linear scan over every row, not a sorted/binary-search
            # lookup, specifically so overlapping ranges still surface as MULTIPLE rather than
            # silently returning whichever one a binary search happened to land on first.
            matches = [row for row in self.geo_info if row['ip_from'] <= ip_int <= row['ip_to']]
            nresults = len(matches)
            if nresults == 0:
                rval = IP2Geo.unknown_geo
            elif nresults > 1:
                rval = IP2Geo.unknown_multiple_geo
            else:
                fr = matches[0]
                rval = {'country_code': fr['country_code'], 'country_name': fr['country_name'], 'region_name': fr['region_name'], 'city_name': fr['city_name'], 'zip_code': fr['zip_code']}

        self.ip_cache[ip_addrL] = rval
        return rval
#a few tests
if __name__ == "__main__":
    #Instantiate an IP2Geo class, give it the tsv file to read IP info from
    #
    #provided as a tarred file currently, must untar  this file into a directory because
    #the uncompressed version is too big to directly commit to github
    #
    #ALSO IMPORTANT: This file is checked into a private repo currently, never post anywhere
    #publicly (github public repo or other..) because it would break the license of the file
    #that was obtained from IP2Location.com
    ipgi = IP2Geo("IP2LOCATION-LITE-DB11.tsv")

    print(ipgi.get_ip_geo_info("125.346.789.121"))
    print(ipgi.get_ip_geo_info("127.0.0.1"))
    print(ipgi.get_ip_geo_info("130.29.15.2"))
    print(ipgi.get_ip_geo_info("136.142.159.55"))
    print(ipgi.get_ip_geo_info("10.12.14.15"))
    print(ipgi.get_ip_geo_info("74.109.247.180"))
    print(ipgi.get_ip_geo_info("192.231.243.233"))
    print(ipgi.get_ip_geo_info("171.67.99.198"))

#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: offline_scan

:Synopsis:
 
:Author:
    servilla
    Duane Costa

:Created:
    1/28/17
"""

from docopt import docopt
import json
import logging
from lxml import etree
import requests
import sys

logging.basicConfig(format='%(asctime)s %(levelname)s (%(name)s): %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S%z', 
                    # filename='offline_data' + '.log',
                    level=logging.INFO)

logger = logging.getLogger('offline_data')


def get_identifiers(base_url=None, scope=None):
    """
    Gets the list of identifiers for a given scope from PASTA
    """
    try:
        url = base_url + '/package/eml/' + scope
        r = requests.get(url)
        if r.status_code != requests.codes.ok:
            logger.error('Bad status code ({code}) for {url}'.format(
                code=r.status_code, url=url))
        else:
            return [_.strip() for _ in (r.text).split('\n')]
    except Exception as e:
        logger.error(e)


def get_newest_revision(base_url=None, scope=None, identifier=None):
    """
    Gets the newest revision of a data package from PASTA
    """
    url = base_url + '/package/eml/' + scope + '/' + identifier \
          + '?filter=newest'
    try:
        r = requests.get(url)
        if r.status_code != requests.codes.ok:
            logger.error(
                'Bad status code ({code}) for {url}'.format(
                    code=r.status_code, url=url))
        else:
            return r.text.strip()
    except Exception as e:
        logger.error(e)


def get_scopes(base_url=None):
    """
    Gets the complete list of scopes from PASTA
    """
    try:
        url = base_url + '/package/eml'
        r = requests.get(url)
        if r.status_code != requests.codes.ok:
            logger.error('Bad status code ({code}) for {url}'.format(
                code=r.status_code, url=url))
        else:
            return [_.strip() for _ in (r.text).split('\n')]
    except Exception as e:
        logger.error(e)
        
        
def package_id_to_path(package_id=None):
    """
    Derives a slash-separated path from a dot-separated package ID
    """
    return package_id.replace('.', '/')


def get_resource_dict(package_id, resource_id, object_name=None, medium_name=None):
    """
    Derives a resource_dict dictionary from the supplied package ID,
    resource ID, and offline XML values
    """
    resource_dict = {"package_id" : package_id,
                     "resource_id" : resource_id,
                     "object_name" : object_name,
                     "medium_name" : medium_name
                    }
    return resource_dict


def scan_for_offline(base_url=None, fp=sys.stdout, scopes=None, black_list=None):
    """
    Scans data packages for offline entities
    """
    offline = []
    unparsed = []
    
    for scope in scopes:
        if scope not in black_list:
            identifiers = get_identifiers(base_url=base_url, scope=scope)
            for identifier in identifiers:
                revision = get_newest_revision(base_url=base_url,
                                               scope=scope,
                                               identifier=identifier)
                package_id = scope + '.' + identifier + '.' + revision
                path = package_id_to_path(package_id)
                metadata_url = base_url + '/package/metadata/eml/' + path

                try:
                    tree = etree.parse(metadata_url)
                    
                    elem = tree.find('//distribution/offline/mediumName')
                    if elem is not None:
                        medium_name = elem.text
                        object_name_elem = tree.find('//distribution/offline/../../objectName')
                        object_name = object_name_elem.text
                        rdict = get_resource_dict(package_id, metadata_url, object_name, medium_name)
                        offline.append(rdict)
         
                except Exception as e:
                    logger.error(e)
                    rdict = get_resource_dict(package_id, metadata_url, "", "")
                    unparsed.append(rdict)
    
    scanned_resources = {}
    scanned_resources["offline"] = offline
    scanned_resources["unparsed"] = unparsed
    json.dump(scanned_resources, fp, indent=2, separators=(',', ': '))
    print('', file=fp)


def main():
    """
    Reports on PASTA data packages with offline data entities.

    Usage:
        offline_data.py [-u | --url <url>]  [-s | --scope <scope>]  [-o | --output <output>]
        offline_data.py -h | --help

    Options:
        -u --url      Base URL of PASTA services, e.g. 'https://pasta.lternet.edu'
        -s --scope    Restrict to given scope
        -o --output   Output results to file; the filename should have a .json extension
        -h --help     This page

    """
    black_list = ('lter-landsat', 'lter-landsat-ledaps', 'ecotrends')
    args = docopt(str(main.__doc__))

    url = args['<url>']
    scope = args['<scope>']
    output = args['<output>']

    if not url:
        BASE_URL = "http://localhost:8888"
    else :
        BASE_URL = url
    
    if scope is None:
        scopes = get_scopes(base_url=BASE_URL)
    else:
        scopes = [scope]

    if output is None:
        fp = sys.stdout
    else:
        fp = open(output, 'w')

    scan_for_offline(base_url=BASE_URL, fp=fp, scopes=scopes, black_list=black_list)

    if (output):
        fp.close()
        
    logger.info("Finished program")


if __name__ == "__main__":
    main()

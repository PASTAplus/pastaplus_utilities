# -*- coding: utf-8 -*-

""":Mod: public_no_access_json

:Synopsis:
    Reports PASTA metadata and data resources that lack public read access,
    outputs the report as JSON formatted text.

:Author:
    Duane Costa

:Created:
    2/16/18
"""

import logging
import os
import sys
from base64 import b64decode, b64encode
from docopt import docopt
import requests
import xml.etree.ElementTree as ET
import json

logging.basicConfig(format='%(asctime)s %(levelname)s (%(name)s): %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S%z',
                    # filename='$NAME' + '.log',
                    level=logging.INFO)

logger = logging.getLogger('public_no_access_report')


def get_metadata_acl(base_url=None, path=None, cookies=None):
    """
    Gets the metadata access control block from PASTA
    """
    url = base_url + '/package/metadata/acl/eml/' + path
    try:
        r = requests.get(url, cookies=cookies)
        if r.status_code != requests.codes.ok:
            logger.error(
                'Bad status code ({code}) for {url}'.format(
                    code=r.status_code, url=url))
        else:
            return r.text.strip()
    except Exception as e:
        logger.error(e)


def get_data_acl(base_url=None, path=None, cookies=None):
    """
    Gets the data access control block from PASTA
    """
    url = base_url + '/package/data/acl/eml/' + path
    try:
        r = requests.get(url, cookies=cookies)
        if r.status_code != requests.codes.ok:
            logger.error(
                'Bad status code ({code}) for {url}'.format(
                    code=r.status_code, url=url))
        else:
            return r.text.strip()
    except Exception as e:
        logger.error(e)


def get_resource_map(base_url=None, path=None, cookies=None):
    """
    Gets the resource map for a given package ID from PASTA
    """
    resource_map = ""
    url = base_url + '/package/eml/' + path
    
    try:
        r = requests.get(url, cookies=cookies)
        if r.status_code != requests.codes.ok:
            logger.error(
                'Bad status code ({code}) for {url}'.format(
                    code=r.status_code, url=url))
        else:
            resource_map = r.text.strip()
    except Exception as e:
        logger.error(e)
        
    return resource_map


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
        
        
def has_public_read_access(acl_xml=None):
    """
    Boolean to determine whether an access control list XML contains
    an entry for public read access
    """
    has_public_read = False
    try:
        root = ET.fromstring(acl_xml)
        for child in root:
            has_principal_public = False
            has_permission_read = False
            if child.tag == 'allow':
                for grandchild in child:
                    if grandchild.tag == 'principal' and \
                       grandchild.text == 'public':
                        has_principal_public = True
                    if grandchild.tag == 'permission' and \
                       grandchild.text == 'read':
                        has_permission_read = True
                if has_principal_public and has_permission_read:
                    has_public_read = True         
    except Exception as e:
        logger.error(e)
    return has_public_read


def parse_metadata_resource(resource_map=None):
    """
    Returns the metadata resource found in a PASTA resource map
    """
    metadata_resource = ""
    if resource_map:
        resources = resource_map.split('\n')
        for resource in resources:
            if '/metadata/' in resource:
                metadata_resource = resource
                break
    return metadata_resource


def parse_data_resources(resource_map=None):
    """
    Returns a list of data resources found in a PASTA resource map
    """
    data_resources = []
    if resource_map:
        resources = resource_map.split('\n')
        for resource in resources:
            if '/data/' in resource:
                data_resources.append(resource)
    return data_resources


def package_id_to_path(package_id=None):
    """
    Derives a slash-separated path from a dot-separated package ID
    """
    return package_id.replace('.', '/')


def authenticate(base_url=None, creds=None):
    """
    Authenticates with PASTA using the supplied credentials,
    returning the cookies containing the authentication token
    for subsequent usage
    """
    service = '/package/eml'
    
    if (creds):
        dn, pw = tuple(creds.split(':'))
        url = base_url + service
        r = requests.get(url, auth=(dn, pw))
        auth_token = r.cookies['auth-token']
        cookies = {'auth-token' : auth_token}
        return cookies
    else:
        return None


def get_resource_dict(package_id, resource_id, acl_xml):
    """
    Derives a resource_dict dictionary from the supplied package ID,
    resource ID, and access control XML values
    """
    resource_dict = {"package_id" : package_id,
                     "resource_id" : resource_id,
                     "acl_xml" : acl_xml
                    }
    return resource_dict


def main(argv):
    """
    Reports on PASTA metadata and data resources that lack public read access.

    Usage:
        public_no_access_report.py [-u | --url <url>] 
                                   [-c | --creds <creds>]  
                                   [-s | --scope <scope>]  
                                   [-o | --output <output>]
        public_no_access_report.py -h | --help

    Options:
        -u --url      Base URL of PASTA services, 
                      e.g. 'https://pasta.lternet.edu'
        -c --creds    Authentication credentials
        -s --scope    Restrict to given scope
        -o --output   Output results to file; 
                      the filename should have a .json extension
        -h --help     This page

    """
    args = docopt(str(main.__doc__))

    url = args['<url>']
    creds = args['<creds>']
    scope = args['<scope>']
    ignored_scopes = ('lter-landsat', 'lter-landsat-ledaps', 'ecotrends')
    output = args['<output>']

    if not url:
        BASE_URL = "http://localhost:8888"
    else :
        BASE_URL = url
    
    if not creds:
        logger.error("Specify authentication credentials with '-c <creds>'")
        sys.exit()
    
    my_cookies = authenticate(BASE_URL, creds)
    if not my_cookies:
        logger.error("Failed to authenticate")
        sys.exit()
        
    non_public_metadata = []
    non_public_data = []

    if scope is None:
        scopes = get_scopes(base_url=BASE_URL)
    else:
        scopes = [scope]

    if output is None:
        fp = sys.stdout
    else:
        fp = open(output, 'w')

    for scope in scopes:
        if scope not in ignored_scopes:
            identifiers = get_identifiers(base_url=BASE_URL, scope=scope)
    
            for identifier in identifiers:
                revision = get_newest_revision(base_url=BASE_URL,
                                               scope=scope,
                                               identifier=identifier)
                id = scope + '.' + identifier + '.' + revision
                logger.info("Working on package ID: " + id)
                path = package_id_to_path(id)
                # Get the resource map, if we have read access to it
                resource_map = get_resource_map(base_url=BASE_URL,
                                                path=path,
                                                cookies=my_cookies)
                metadata_resource = parse_metadata_resource(resource_map)
                metadata_acl_xml = ""
                
                try:
                    metadata_acl_xml = get_metadata_acl(base_url=BASE_URL, 
                                                        path=path, 
                                                        cookies=my_cookies)
                    if has_public_read_access(metadata_acl_xml):
                        # Get the data resources from the resource map
                        data_resources = parse_data_resources(resource_map)
                        for data_resource in data_resources:
                            entity_id = data_resource.split('/')[-1]
                            entity_path = path + '/' + entity_id
                            try:
                                data_acl_xml = get_data_acl(base_url=BASE_URL,
                                                            path=entity_path,
                                                            cookies=my_cookies)
                                if not has_public_read_access(data_acl_xml):
                                    rdict = get_resource_dict(id, 
                                                              data_resource, 
                                                              data_acl_xml)
                                    non_public_data.append(rdict)
                            except Exception as e:
                                logger.error(e)
                                rdict = get_resource_dict(id, 
                                                          data_resource, 
                                                          data_acl_xml)
                                non_public_data.append(rdict)
                    else:
                        rdict = get_resource_dict(id, 
                                                  metadata_resource, 
                                                  metadata_acl_xml)
                        non_public_metadata.append(rdict)
                except Exception as e:
                    logger.error(e)
                    rdict = get_resource_dict(id, 
                                              metadata_resource, 
                                              metadata_acl_xml)
                    non_public_metadata.append(rdict)
            
    non_public_resources = {}
    non_public_resources["metadata"] = non_public_metadata
    non_public_resources["data"] = non_public_data
    
    json.dump(non_public_resources, fp, indent=2, separators=(',', ': '))
    print('', file=fp)
    if (output):
        fp.close()
        
    logger.info("Finished program")
       

if __name__ == "__main__":
    main(sys.argv)

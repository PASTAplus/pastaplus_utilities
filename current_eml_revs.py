#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: current_eml_revs

:Synopsis:
 
:Author:
    servilla
    Duane Costa

:Created:
    1/28/17
"""

from docopt import docopt
import glob
import json
import logging
import os
import requests
import sys

logging.basicConfig(format='%(asctime)s %(levelname)s (%(name)s): %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S%z',
                    # filename='current_eml_revs' + '.log',
                    level=logging.INFO)

logger = logging.getLogger('current_eml_revs')


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


def get_resource_dict(package_id, 
                      resource_id, 
                      object_name=None, 
                      medium_name=None):
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


def file_to_do(output_dir: str, 
               fp: str, 
               scope: str, 
               identifier: str, 
               revision: str) -> bool:
    # If another revision's eml file exists, delete it
    # If this revision's eml file already exists, return False, otherwise True.
    pattern = f'{output_dir}/{scope}.{identifier}.*.eml'
    this_rev_exists = False
    revs = glob.glob(pattern)
    for file in revs:
        if os.path.isfile(file):
            substrs = file.split('.')
            rev = substrs[-2]
            if rev == revision:
                this_rev_exists = True
            else:
                print(f'Removing {file}', file=fp)
                try:
                    os.remove(file)
                except Exception as e:
                    print(e, file=fp)
                    logger.error(e)
    return not this_rev_exists


def get_current_eml_revs(base_url=None, 
                         output_dir=None,
                         fp=None,
                         scopes=None, 
                         black_list=None):
    """
    Gets EML for newest revisions of data packages
    """

    unparsed = []

    for scope in scopes:
        if scope not in black_list:
            identifiers = get_identifiers(base_url=base_url, scope=scope)
            for identifier in identifiers:
                revision = get_newest_revision(base_url=base_url,
                                               scope=scope,
                                               identifier=identifier)
                # print(scope, identifier, revision)
                package_id = scope + '.' + identifier + '.' + revision
                if not file_to_do(output_dir=output_dir, 
                                  fp=fp, 
                                  scope=scope, 
                                  identifier=identifier, 
                                  revision=revision):
                    continue
                path = package_id_to_path(package_id)
                metadata_url = base_url + '/package/metadata/eml/' + path
                try:
                    r = requests.get(metadata_url)
                    if r.status_code == requests.codes.ok:
                        eml = r.text
                        print(package_id, file=fp)
                        output_filepath = f'{output_dir}/{package_id}.eml'
                        with open(output_filepath, 'w') as output_file:
                            output_file.write(eml)
                    else:
                        rdict = get_resource_dict(package_id, metadata_url, "", "")
                        unparsed.append(rdict)
                except Exception as e:
                    logger.error(e)

    scanned_resources = {}
    scanned_resources["unparsed"] = unparsed
    json.dump(scanned_resources, fp, indent=2, separators=(',', ': '))
    print('', file=fp)


def main():
    """
    Collects the EML for the current rev of each data package

    Usage:
        current_eml_revs.py [-u | --url <url>]  
                            [-s | --scope <scope>]  
                            [-d | --dir <dir>]
                            [-o | --output <output>]
        current_eml_revs.py -h | --help

    Options:
        -u --url        Base URL of PASTA services; 
                        default = 'https://pasta.lternet.edu'
        -s --scope      Restrict to given scope. If unspecified,
                        get all scopes
        -d --dir        Save the results to files in given dir.
                        Directory is assumed to exist; default= "eml" 
        -o --output     Print destination; default=stdout
        -h --help       This page

    """
    logger.info("Started program")

    black_list = ('lter-landsat', 'lter-landsat-ledaps', 'ecotrends')
    args = docopt(str(main.__doc__))

    url = args['<url>']
    scope = args['<scope>']
    output_dir = args['<dir>']
    output = args['<output>']

    if not url:
        BASE_URL = "https://pasta.lternet.edu"
    else :
        BASE_URL = url
    
    if scope is None:
        scopes = get_scopes(base_url=BASE_URL)
    else:
        scopes = [scope]

    if not output_dir:
        output_dir = "eml"

    if not output:
        fp = sys.stdout
    else:
        fp = open(output, 'w')

    get_current_eml_revs(base_url=BASE_URL, 
                         output_dir=output_dir,
                         fp=fp,
                         scopes=scopes, 
                         black_list=black_list)

    logger.info("Finished program")


if __name__ == "__main__":
    main()

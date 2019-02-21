#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: eml_gettr

:Synopsis:
    Download EML from packages specified by Solr query to local directory
    usage: eml_gettr.py --verbose --count=1 -env=production /tmp/eml

:Author:
    servilla

:Created:
    12/5/18
"""
import logging
import os

import click
import daiquiri
from lxml import etree
import requests

daiquiri.setup(level=logging.INFO,
               outputs=(
                   'stderr',
               ))
logger = daiquiri.getLogger('eml_gettr: ' + __name__)


def get_pids(env: str, count: int, fq: str) -> list:
    solr_url = f'{env}/search/eml?defType=edismax&q=*&' + \
               f'{fq}fl=packageid&' + \
               f'&debug=false&rows={count}&sort=packageid,asc'

    r = requests.get(solr_url)
    if r.status_code == requests.codes.ok:
        result_set = r.text.encode('utf-8')
    else:
        raise requests.exceptions.ConnectionError()

    root = etree.fromstring(result_set)
    _ = root.findall('.//packageid')
    pids = list()
    for pid in _:
        pids.append(pid.text)

    return pids


def get_eml(pid: str, env: str) -> str:
    eml = None
    package_path = pid.replace('.', '/')
    eml_url = f'{env}/metadata/eml/{package_path}'
    r = requests.get(eml_url)
    if r.status_code == requests.codes.ok:
        eml = r.text
    return eml


env_help = 'PASTA+ environment to query: production, staging, development'
count_help = 'Number of EML documents to return'
include_help = 'Include scope'
exclude_help = 'Exclude scope(s)'
verbose_help = 'Display event information'


@click.command()
@click.argument('eml_dir')
@click.option('-e', '--env', default='production', help=env_help)
@click.option('-c', '--count', default=10000, help=count_help)
@click.option('-i', '--include', default='', help=include_help)
@click.option('-e', '--exclude', multiple=True, help=exclude_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def main(eml_dir: str, env: str, count: int, include: tuple, exclude: tuple,
         verbose: bool):
    if not os.path.isdir(eml_dir):
        logger.error(f'EML directory "{eml_dir}" does not exist')
        exit(1)

    pasta = None
    if env == 'production':
        pasta = 'https://pasta.lternet.edu/package'
    elif env == 'staging':
        pasta = 'https://pasta-s.lternet.edu/package'
    elif env == 'development':
        pasta = 'https://pasta-d.lternet.edu/package'
    else:
        logger.error(f'PASTA environment "{env}" not recognized')
        exit(1)

    # Create include/exclude filter query for scope values
    fq = ''
    if include != '': fq = f'fq=scope:{include}&'
    fq += ''.join(['fq=-scope:' + _.strip() + '&' for _ in exclude])

    pids = get_pids(pasta, count, fq)
    npids = len(pids)
    n = 1
    for pid in pids:
        percent = int(n / npids * 100)
        msg = f'Downloading {pid}: {n}/{npids - n} ({percent}%) - '
        eml = get_eml(pid, pasta)
        if eml is not None:
            with open(f'{eml_dir}/{pid}.xml', 'w') as f:
                msg += f'writing'
                f.write(eml)
        else:
            msg += f'failed to access'
        if verbose:
            click.echo(msg)
        n += 1

    return 0


if __name__ == "__main__":
    main()

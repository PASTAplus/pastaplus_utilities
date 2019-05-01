#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: eml_gettr

:Synopsis:
    Download EML from packages specified by Solr query to local directory
    usage: eml_gettr.py --verbose --count=1 --block_size=10 --env=production /tmp/eml

:Author:
    servilla

:Created:
    12/5/18

:Modified:
    2/21/19
"""
import asyncio
import logging
from pathlib import Path

import aiohttp
from aiohttp.client_exceptions import ClientError
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


async def get_eml(pid: str, pasta: str) -> str:
    package_path = pid.replace('.', '/')
    eml_url = f'{pasta}/metadata/eml/{package_path}'
    async with aiohttp.ClientSession() as session:
        async with session.get(eml_url) as resp:
            resp.raise_for_status()
            return await resp.text()


async def get_block(pids: list, pasta: str, e_dir: str, verbose: bool):
    tasks = []

    for pid in pids:
        tasks.append((pid, loop.create_task(get_eml(pid, pasta))))

    for pid, t in tasks:
        try:
            eml = await t
            file_path = f'{e_dir}/{pid}.xml'
            with open(file_path, 'w') as f:
                f.write(eml)
                msg = f'Writing: {file_path}'
                if verbose:
                    click.echo(msg)
        except (ClientError, IOError):
            logger.error(f'Failed to access or write: {pid}')


env_help = 'PASTA+ environment to query: production, staging, development'
count_help = 'Number of EML documents to return'
include_help = 'Include scope'
exclude_help = 'Exclude scope(s)'
verbose_help = 'Display event information'
block_size_help = 'Number of concurrent requests to PASTA'


@click.command()
@click.argument('e_dir')
@click.option('-e', '--env', default='production', help=env_help)
@click.option('-c', '--count', default=10000, help=count_help)
@click.option('-i', '--include', default='', help=include_help)
@click.option('-x', '--exclude', multiple=True, help=exclude_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('-b', '--block_size', default=5, help=block_size_help)
def main(e_dir: str, env: str, count: int, include: tuple, exclude: tuple,
         verbose: bool, block_size: int):
    if not Path(e_dir).is_dir():
        logger.error(f'Directory "{e_dir}" does not exist')
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

    global loop
    loop = asyncio.get_event_loop()

    # Execute N blocks of pids concurrently
    i, j = 0, block_size
    while i < len(pids):
        loop.run_until_complete(get_block(pids[i:j], pasta, e_dir, verbose))
        i, j = j, j + block_size

    return 0


if __name__ == "__main__":
    main()

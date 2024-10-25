#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: eml_gettr

:Synopsis:
    Download EML from packages specified by Solr query to local directory
    usage: eml_gettr.py --verbose --count=1 --block_size=5 --env=production /tmp/eml

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
import os

import aiohttp
import click
import daiquiri
from lxml import etree
import requests
from sqlalchemy.testing.config import ident

cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/eml_gettr.log"
daiquiri.setup(level=logging.INFO,
               outputs=(daiquiri.output.File(logfile), "stdout",))
logger = daiquiri.getLogger(__name__)

def get_pids_solr(env: str, count: int, include: tuple, exclude: tuple, verbose: bool) -> list:
    """get_pids_solr
    Returns only the most recent revisions of data packages for given scopes.
    """

    # Create include/exclude filter query for scope values
    fq = "".join(["fq=scope:" + _.strip() + "&" for _ in include])
    if fq != "":
        fq += "&"
    fq += "".join(["fq=-scope:" + _.strip() + "&" for _ in exclude])

    solr_url = f'{env}/search/eml?defType=edismax&q=*&' + \
               f'{fq}fl=packageid&' + \
               f'&debug=false&rows={count}&sort=packageid,asc'

    r = requests.get(solr_url)
    r.raise_for_status()
    result_set = r.text.encode('utf-8')
    root = etree.fromstring(result_set)
    _ = root.findall('.//packageid')
    pids = []
    for pid in _:
        pids.append(pid.text)

    return pids


def get_pids_pasta(env: str, count: int, include: tuple, exclude: tuple, verbose: bool) -> list:
    """get_pids_pasta
    Return all revisions of data packages for given scopes.
    """

    include = set(include)
    exclude = set(exclude)

    # Get superset of scope values
    scope_url = f"{env}/eml"
    req = requests.get(scope_url)
    req.raise_for_status()
    scopes = set(req.text.split("\n"))

    if len(include) != 0:
        scopes = (scopes & include) - exclude
    else:
        scopes = scopes - exclude

    c = 0
    pids = []
    for s in scopes:
        identifier_url = f"{env}/eml/{s}"
        req = requests.get(identifier_url)
        req.raise_for_status()
        identifiers = req.text.split("\n")
        for i in identifiers:
            revisions_url = f"{env}/eml/{include}/{i}"
            req = requests.get(revisions_url)
            req.raise_for_status()
            revisions = req.text.split("\n")
            for r in revisions:
                c += 1
                if c > count:
                    return pids
                pid = f"{include}.{i}.{r}"
                if verbose:
                    print(f"Adding pid: {pid}")
                pids.append(pid)

    return pids


async def get_eml(pid: str, pasta: str) -> str:
    package_path = pid.replace('.', '/')
    eml_url = f'{pasta}/metadata/eml/{package_path}'
    async with aiohttp.ClientSession() as session:
        async with session.get(eml_url) as resp:
            resp.raise_for_status()
            return await resp.text()


async def get_request_block(pids: list, pasta: str, e_dir: str, verbose: bool):
    tasks = []

    for pid in pids:
        tasks.append((pid, loop.create_task(get_eml(pid, pasta))))

    for pid, t in tasks:
        try:
            eml = await t
            file_path = f'{e_dir}/{pid}.xml'
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(eml)
                msg = f'Writing: {file_path}'
                if verbose:
                    print(msg)
        except Exception as e:
            msg = f'Failed to access or write: {pid}\n{e}'
            logger.error(msg)


env_help = "PASTA+ environment to query: production (default), staging, development"
count_help = "Number of EML documents to return (default 10,000)"
all_help = "Include all revisions"
include_help = "Include scope(s) e.g. -i scope_1 -i scope_2 ..."
exclude_help = "Exclude scope(s) e.g. -x scope_1 -x scope_2 ..."
verbose_help = "Display event information"
request_size_help = "Number of concurrent requests to PASTA (default 5)"
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('e_dir')
@click.option('-e', '--env', default='production', help=env_help)
@click.option('-c', '--count', default=10000, help=count_help)
@click.option('-a', '--all', is_flag=True, help=all_help)
@click.option('-i', '--include', multiple=True, help=include_help)
@click.option('-x', '--exclude', multiple=True, help=exclude_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
@click.option('-r', '--request_size', default=5, help=request_size_help)
def main(
        e_dir: str,
        env: str,
        count: int,
        all: bool,
        include: tuple,
        exclude: tuple,
        verbose: bool,
        request_size: int
):

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

    if all:
        pids = get_pids_pasta(pasta, count, include, exclude, verbose)
    else:
        pids = get_pids_solr(pasta, count, include, exclude, verbose)

    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Execute N requests of pids concurrently
    i, j = 0, request_size
    while i < len(pids):
        loop.run_until_complete(get_request_block(pids[i:j], pasta, e_dir, verbose))
        i, j = j, j + request_size

    return 0


if __name__ == "__main__":
    main()

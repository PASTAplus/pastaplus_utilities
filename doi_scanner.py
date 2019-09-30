#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gets DOIs for data packages known to PASTA and checks that they reference an existing online landing page.

Results are saved in files, one file per scope. File contents are:
    package ID          DOI         status (one of: OK, NOT PUBLIC, NOT FOUND)
"""

from datetime import datetime
import glob
import time
import sys

from aiohttp import ClientSession
import asyncio
import requests

import click


@click.command()
@click.option(
    "-p",
    default="pasta.lternet.edu",
    help="PASTA server (default is pasta.lternet.edu)"
)
@click.option(
    "-o",
    default="dois",
    help="Directory to receive DOI files (default is 'dois')"
)
@click.option(
    "-s",
    default=None,
    help="Scope(s) to be scanned. Comma-separated. If None, all scopes are scanned."
)
@click.option(
    "-d",
    default="shallow",
    help="Depth: 'deep' or 'shallow'. If shallow, skip DOIs that have resolved successfully in the past. Default is 'shallow'. "
)
@click.option(
    "-r",
    default="doi_report.txt",
    help="Filename for summary report, saved in -d option directory. Default is 'doi_report.txt."
)
@click.option(
    "-x",
    default=None,
    help="Name of file containing list of package IDs to exclude."
)
def doi_scanner(p: str, o: str, s: str, d: str, r: str, x: str):
    """
    Get the DOIs associated with PASTA data packages. Save them in files and generate a summary report.
    """
    main(p, o, s, d, r, x)


BURST_SIZE = 5
MAX_RETRIES = 3
TIME_TO_SLEEP = 1

pasta_server = None
scopes = []
deep = False
exclude = []
packages = []
dois = {}


# Get a list of the revisions for a scope.identifier from PASTA.
async def get_revision_list(session, scope, identifier):
    url = "https://{}/package/eml/{}/{}".format(pasta_server, scope, identifier)
    resp = await session.request(method='GET', url=url)
    resp.raise_for_status()
    return await resp.text()


# Asynchronously get revisions for a scope.identifier and save each scope.identifier.revision
#  in the packages list.
async def save_revisions(session, id):
    scope, identifier = id
    retries = 0
    while retries < MAX_RETRIES:
        try:
            resp = await get_revision_list(session, scope, identifier)
            break
        except:
            print('{}.{} - Getting revisions - Exception: {}'.format(scope, identifier, sys.exc_info()[0]), flush=True)
            retries += 1
            if retries >= MAX_RETRIES:
                print('Reached max retries for {}.{}... giving up'.format(scope, identifier), flush=True)
                return
            time.sleep(TIME_TO_SLEEP)
    for revision in resp.split('\n'):
        packages.append((scope, int(identifier), int(revision)))


# Asynchronously get the revisions for a list of (scope, identifier) pairs.
async def run_get_revisions_tasks(ids):
    async with ClientSession() as session:
        tasks = [save_revisions(session, id) for id in ids]
        await asyncio.gather(*tasks)


# Get all of the revisions for the identifiers within a scope.
def get_revisions(scope, identifiers):
    count = 0
    ids = []
    for identifier in identifiers:
        ids.append((scope, identifier))
        count += 1
        if count % BURST_SIZE == 0:
            asyncio.run(run_get_revisions_tasks(ids))
            time.sleep(TIME_TO_SLEEP)
            ids = []
    asyncio.run(run_get_revisions_tasks(ids))


# Get the DOI and its status for a scope.identifier.revision.
async def get_doi(session, scope, identifier, revision):
    if not deep:
        print('Getting DOI for {}.{}.{}'.format(scope, identifier, revision))
    url = "https://{}/package/doi/eml/{}/{}/{}".format(pasta_server, scope, identifier, revision)
    resp = await session.request(method='GET', url=url)
    doi = await resp.text()
    if resp.status == 401:
        return "N/A", "NOT PUBLIC"
    resp.raise_for_status()
    status = None
    url = "http://dx.doi.org/" + doi
    resp = await session.request(method="GET", url=url)
    resp.raise_for_status()
    html = await resp.text()
    if html:
        if "Data Package Summary" in html:
            status = "OK"
        else:
            status = "NOT FOUND"
    return doi, status


# Asynchronously get DOIs and their statuses for a scope.identifier.revision and save them in the dois dictionary
#  keyed by scope.identifier.revision.
async def save_dois(session, id):
    scope, identifier, revision = id
    retries = 0
    while retries < MAX_RETRIES:
        try:
            resp = await get_doi(session, scope, identifier, revision)
            break
        except:
            print('{}.{}.{} - Getting DOI - Exception: {}'.format(scope, identifier, revision, sys.exc_info()[0]), flush=True)
            retries += 1
            if retries >= MAX_RETRIES:
                print('Reached max retries getting DOI for {}.{}.{}... giving up'.format(scope, identifier, revision), flush=True)
                return
            time.sleep(TIME_TO_SLEEP)
    doi, status = resp
    dois[(scope, identifier, revision)] = (doi, status)


# Asynchronously get the DOIs/statuses for a list of (scope, identifier, revision) tuples.
async def run_get_dois_tasks(ids):
    async with ClientSession() as session:
        tasks = [save_dois(session, id) for id in ids]
        await asyncio.gather(*tasks)


# Get all of the DOIs/statuses for all of the packages in the packages list.
def get_dois():
    count = 0
    ids = []
    for scope, identifier, revision in packages:
        if (scope, identifier, revision) not in dois:
            ids.append((scope, identifier, revision))
        else:
            doi, status = dois[(scope, identifier, revision)]
            if not deep and status == "OK":
                continue
        count += 1
        if count % BURST_SIZE == 0:
            asyncio.run(run_get_dois_tasks(ids))
            time.sleep(TIME_TO_SLEEP)
            ids = []
        if count % 1000 == 0:
            now = datetime.now().strftime('%H:%M:%S')
            print('{}  {}\tcount={}'.format(now, scope, count, flush=True))
    asyncio.run(run_get_dois_tasks(ids))


# Get a list of all the scopes in PASTA.
def get_scope_list():
    url = "https://{}/package/eml".format(pasta_server)
    scopes = requests.get(url).text.split('\n')
    return scopes


# Get a list of all of the identifiers for a scope in PASTA.
def get_identifier_list(scope: str):
    url = "https://{}/package/eml/{}".format(pasta_server, scope)
    identifiers = requests.get(url).text.split('\n')
    return identifiers


def read_doi_cache(doi_dir: str):
    path = "{}/*.dois".format(doi_dir)
    for filename in glob.glob(path):
        with open(filename, 'r') as doi_file:
            lines = doi_file.readlines()
            for line in lines:
                package_id, doi, status = line.rstrip().split('\t')
                if status == "OK":
                    scope, identifier, revision = package_id.split('.')
                    dois[(scope, int(identifier), int(revision))] = (doi, status)


def write_report(report_filepath: str):
    not_public = []
    not_found = []
    for key, val in dois.items():
        source, identifier, revision = key
        package_id = '{}.{}.{}'.format(source, identifier, revision)
        doi, status = val
        if status == 'NOT PUBLIC':
            not_public.append((package_id, doi))
        elif status == 'NOT FOUND':
            not_found.append((package_id, doi))

    with open(report_filepath, 'w') as report_file:
        report_file.write('NOT PUBLIC:\n')
        for package_id, _ in not_public:
            report_file.write('{}\n'.format(package_id))
        report_file.write('NOT FOUND:\n')
        for package_id, _ in not_found:
            report_file.write('{}\n'.format(package_id))


def main(server: str, doi_dir: str, scope: str, depth: str, report_filename: str, exclude_filename: str):
    global pasta_server, scopes, deep, exclude

    if depth.lower() == "deep":
        deep = True

    if exclude_filename:
        with open(exclude_filename, 'r') as exclude_file:
            lines = exclude_file.readlines()
            for line in lines:
                exclude.append(line.rstrip())

    read_doi_cache(doi_dir)

    pasta_server = server
    if scope is None:
        scopes = get_scope_list()
    else:
        scopes = scope.split(',')

    for scope in scopes:
        identifiers = get_identifier_list(scope)
        now = datetime.now().strftime('%H:%M:%S')
        print('{}  {}\t[{}]'.format(now, scope, str(len(identifiers))), flush=True)
        get_revisions(scope, identifiers)
        packages.sort()
        get_dois()
        output_filename = doi_dir + '/' + scope + '.dois'
        with open(output_filename, 'w') as output_file:
            for scope, identifier, revision in packages:
                if (scope, identifier, revision) in dois:
                    doi, status = dois[(scope, identifier, revision)]
                else:
                    doi, status = "NA", "NA"
                output_file.write('{}.{}.{}\t{}\t{}\n'.format(scope, identifier, revision, doi, status))
        packages.clear()
    write_report('{}/{}'.format(doi_dir, report_filename))
    print(datetime.now().strftime('%H:%M:%S'), flush=True)


if __name__ == '__main__':
    doi_scanner()


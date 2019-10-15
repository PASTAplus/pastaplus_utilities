#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gets DOIs for data packages known to PASTA and checks that they reference an existing online landing page.

Results are cached in file doi_cache.tsv. File contents are:
    package ID          DOI         status (one of: OK, NOT RESOLVED, MISSING)

Example:
    python doi_scanner.py -r ../dashboard/webapp/reports/doi_report.json -x doi_exclude.txt
"""

from datetime import datetime
from collections import OrderedDict
import requests

import click
from sqlalchemy import create_engine

import config


@click.command()
@click.option(
    "-d",
    default="shallow",
    help="Depth: 'shallow', 'deep', 'report_only'. If shallow, skip DOIs that have resolved successfully in the past. Default is 'shallow'. "
)
@click.option(
    "-r",
    default="./doi_report.json",
    help="Filepath for summary report. Default is './doi_report.json'."
)
@click.option(
    "-x",
    default=None,
    help="Name of file containing list of package IDs to exclude."
)
def doi_scanner(d: str, r: str, x: str):
    """
    Gets DOIs for data packages known to PASTA and checks that they reference an existing online landing page.

    Results are cached in file doi_cache.tsv. File contents are:\n
        package ID          DOI         status (one of: OK, NOT RESOLVED, MISSING)

    Example:\n
        python doi_scanner.py -r ../dashboard/webapp/reports/doi_report.json -x doi_exclude_list.txt
    """
    main(d, r, x)


ignored_scopes = () #('lter-landsat', 'lter-landsat-ledaps', 'ecotrends')

deep = False
report_only = False
exclude = []
packages = OrderedDict()
dois = {}


def connect():
    db = config.DB_DRIVER + '://' + \
        config.DB_USER + ':' + \
        config.DB_PW + '@' + \
        config.DB_HOST + '/' + \
        config.DB_DB

    connection = create_engine(db)
    return connection


def get_packages(connection, ignored_scopes):
    sql = ("select scope, identifier, revision, doi, date_deactivated from datapackagemanager.resource_registry "
           "where resource_type='dataPackage' order by scope, identifier, revision")
    result_set = connection.execute(sql).fetchall()
    for scope, identifier, revision, doi, date_deactivated in result_set:
        if scope in ignored_scopes:
            continue
        package_id = f'{scope}.{identifier}.{revision}'
        if not package_id in exclude:
            packages[(scope, identifier, revision)] = (doi, date_deactivated)


def check_doi(doi):
    status = "MISSING"
    if doi:
        url = "http://dx.doi.org/" + doi
        html = requests.get(url).text
        if html:
            if "Data Package Summary" in html:
                status = "OK"
            else:
                status = "NOT RESOLVED"
    return status


def get_dois(connection):
    for scope, identifier, revision in packages:
        doi, date_deactivated = packages[(scope, identifier, revision)]
        if (scope, identifier, revision) in dois:
            _, status = dois[(scope, identifier, revision)]
            if not deep and status == "OK":
                continue
        print(f'Checking {scope}.{identifier}.{revision} - doi={doi}')
        status = check_doi(doi)
        dois[(scope, identifier, revision)] = (doi, status)


def read_doi_cache(cache_filename):
    try:
        with open(cache_filename, 'r') as doi_cache:
            lines = doi_cache.readlines()
            for line in lines:
                package_id, doi, status = line.rstrip().split('\t')
                if status == "OK" or report_only:
                    scope, identifier, revision = package_id.split('.')
                    if scope not in ignored_scopes:
                        dois[(scope, int(identifier), int(revision))] = (doi, status)
    except FileNotFoundError:
        pass


def write_report_segment(report_file, package_list, final_segment=False):
    index = 1
    for (source, identifier, revision), _ in package_list:
        package_id = f'{source}.{identifier}.{revision}'
        if index < len(package_list):
            terminator = '},'
        else:
            terminator = '}'
        index += 1
        report_file.write(f'    {{\n      "package_id": "{package_id}"\n    {terminator}\n')
    if not final_segment:
        report_file.write('  ],\n')
    else:
        report_file.write('  ]\n}\n')


def write_report(report_filepath: str):
    not_resolved = []
    not_resolved_deactivated = []
    missing = []
    missing_deactivated = []

    for key, val in dois.items():
        source, identifier, revision = key
        doi, status = val
        if status == 'NOT RESOLVED':
            _, date_deactivated = packages[(source, identifier, revision)]
            if not date_deactivated:
                not_resolved.append((key, doi))
            else:
                not_resolved_deactivated.append((key, doi))
        if status == 'MISSING':
            _, date_deactivated = packages[(source, identifier, revision)]
            if not date_deactivated:
                missing.append((key, doi))
            else:
                missing_deactivated.append((key, doi))

    not_resolved.sort()
    not_resolved_deactivated.sort()
    missing.sort()
    missing_deactivated.sort()
    with open(report_filepath, 'w') as report_file:
        report_file.write('{\n  "not_resolved": [\n')
        write_report_segment(report_file, not_resolved)
        report_file.write('  "missing": [\n')
        write_report_segment(report_file, missing)
        report_file.write('  "not_resolved_deactivated": [\n')
        write_report_segment(report_file, not_resolved_deactivated)
        report_file.write('  "missing_deactivated": [\n')
        write_report_segment(report_file, missing_deactivated, True)


def write_doi_cache(cache_filename):
    with open(cache_filename, 'w') as output_file:
        for scope, identifier, revision in packages:
            if (scope, identifier, revision) in dois:
                doi, status = dois[(scope, identifier, revision)]
            else:
                doi, status = "NA", "NA"
            output_file.write(f'{scope}.{identifier}.{revision}\t{doi}\t{status}\n')


def main(depth: str, report_filename: str, exclude_filename: str):
    global deep, report_only, exclude

    print(datetime.now().strftime('%H:%M:%S'), flush=True)

    depth = depth.lower()
    if depth == "deep":
        deep = True
    elif depth == "report_only":
        report_only = True

    if exclude_filename:
        with open(exclude_filename, 'r') as exclude_file:
            lines = exclude_file.readlines()
            for line in lines:
                exclude.append(line.rstrip())

    cache_filename = 'doi_cache.tsv'
    read_doi_cache(cache_filename)

    connection = connect()
    get_packages(connection, ignored_scopes)

    if not report_only:
        get_dois(connection)
        write_doi_cache(cache_filename)
    
    write_report(report_filename)
    print(datetime.now().strftime('%H:%M:%S'), flush=True)


if __name__ == '__main__':
    doi_scanner()

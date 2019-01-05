#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: package_tracker

:Synopsis:

:Author:
    servilla

:Created:
    12/15/18
"""
import logging
from urllib.parse import quote

import click
import daiquiri
from lxml import etree
import requests
import pendulum

daiquiri.setup(level=logging.INFO,
               outputs=(
                   'stderr',
               ))
logger = daiquiri.getLogger('package_tracker: ' + __name__)

INDENT = ' ' * 4


def cn_report(resources: list, d1_url: str) -> str:
    report = f'D1 CN ({d1_url}) objects and replica verify times:\n'
    for resource in resources:
        success, response = get_d1_sysmeta(resource, d1_url)
        if success:
            date_verified = get_d1_date_replica_verified(response)
            report += f'{INDENT}{resource} - {date_verified}\n'
        else:
            report += f'{INDENT}{resource} - {response}\n'
    report += f'D1 CN ({d1_url}) science metadata indexed:\n'
    for resource in resources:
        if 'metadata/eml' in resource:
            success, response = get_d1_solr_result(resource, d1_url)
            if success:
                solr_count = get_d1_solr_count(response)
                if solr_count >= 1:
                    report += f'{INDENT}{resource} is indexed'
                else:
                    report += f'{INDENT}{resource} is NOT indexed'
    return report


def get_d1_date_uploaded(sysmeta_xml: str) -> str:
    root = etree.fromstring(sysmeta_xml.encode('utf-8'))
    date_uploaded = root.find('.//dateUploaded')
    return date_uploaded.text


def get_d1_date_replica_verified(sysmeta_xml: str) -> str:
    root = etree.fromstring(sysmeta_xml.encode('utf-8'))
    date_verified = root.find('.//replicaVerified')
    return date_verified.text


def get_d1_solr_count(solr_xml: str) -> int:
    root = etree.fromstring(solr_xml.encode('utf-8'))
    result = root.find('.//result')
    return int(result.get('numFound'))


def get_d1_solr_result(pid: str, d1_url: str) -> tuple:
    pid = quote(f'"{pid}"', safe='')
    url = f'{d1_url}/query/solr/?start=0&rows=10&fl=id%2Ctitle%2CformatId&q=id%3A{pid}'
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        return True, r.text
    elif r.status_code == requests.codes.not_found:
        return False, 'Not Found'
    elif r.status_code == requests.codes.unauthorized:
        return False, 'Unauthorized'
    else:
        return False, f'Unknown error with status code: {r.status_code}'


def get_d1_sysmeta(pid: str, d1_url: str) -> tuple:
    pid = quote(pid, safe='')
    url = f'{d1_url}/meta/{pid}'
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        return True, r.text
    elif r.status_code == requests.codes.not_found:
        return False, 'Not Found'
    elif r.status_code == requests.codes.unauthorized:
        return False, 'Unauthorized'
    else:
        return False, f'Unknown error with status code: {r.status_code}'


def get_package_doi(pid: list, pasta_url: str, auth: tuple) -> str:
    url = pasta_url + f'/doi/eml/{pid[0]}/{pid[1]}/{pid[2]}'
    r = requests.get(url=url, auth=auth)
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        return 'None'


def get_resource_create_date(resource_xml: str) -> str:
    root = etree.fromstring(resource_xml.encode('utf-8'))
    date_created = root.find('.//dateCreated')
    return date_created.text


def get_resource_metadata(pid: list, pasta_url: str, auth: tuple) -> str:
    url = pasta_url + f'/rmd/eml/{pid[0]}/{pid[1]}/{pid[2]}'
    r = requests.get(url=url, auth=auth)
    r.raise_for_status()
    return r.text


def get_resources(pid: list, pasta_url: str, auth: tuple) -> tuple:
    url = pasta_url + f'/eml/{pid[0]}/{pid[1]}/{pid[2]}'
    r = requests.get(url=url, auth=auth)
    if r.status_code == requests.codes.ok:
        return True, r.text
    elif r.status_code == requests.codes.not_found:
        return False, 'Not Found'
    elif r.status_code == requests.codes.unauthorized:
        return False, 'Unauthorized'
    else:
        return False, f'Unknown error with status code: {r.status_code}'


def gmn_report(resources: list, gmn_url: str) -> str:
    report = f'D1 GMN ({gmn_url}) objects and upload times:\n'
    for resource in resources:
        success, response = get_d1_sysmeta(resource, gmn_url)
        if success:
            date_uploaded = pendulum.parse(get_d1_date_uploaded(response))
            date_uploaded_str = date_uploaded.to_iso8601_string()
            report += f'    {resource} - {date_uploaded_str}\n'
        else:
            report += f'    {resource} - {response}\n'
    return report


def pasta_report(pid: str, resources: list, date_created_raw: str) -> str:
    utc = pendulum.timezone('UTC')
    date_created_mt = pendulum.parse(date_created_raw, tz='America/Denver')
    date_created_utc = pendulum.instance(utc.convert(date_created_mt))
    date_created__utc_str = date_created_utc.to_iso8601_string()
    dc_mt = date_created_mt.to_iso8601_string()
    dc_utc = pendulum.parse(date_created__utc_str).to_iso8601_string()

    pid = '.'.join(pid)
    report = f'Package Identifier: {pid}\n'
    report += f'Created: {dc_mt} - {dc_utc}\n'
    report += f'DOI: {resources[-1]}\n'
    report += f'Resources:\n'
    for resource in resources[:-1]:
        report += f'{INDENT}{resource}\n'
    return report


auth_help = 'Basic authentication "user:password"'
env_help = 'PASTA+ environment: production, staging, or development; default is production'


@click.command()
@click.argument('pid')
@click.option('-e', '--env', default='production', help=env_help)
@click.option('-a', '--auth', default=None, help=auth_help)
def track(pid: str, env: str, auth: str):
    pid = pid.split('.')
    if len(pid) != 3:
        msg = f'Package identifier "{pid}" not in correct form'
        raise ValueError(msg)

    if pid[0] == 'edi':
        gmn_host = 'edirepository.org'
    else:
        gmn_host = 'lternet.edu'

    if env == 'production':
        pasta_url = 'https://pasta.lternet.edu/package'
        gmn_url = f'https://gmn.{gmn_host}/mn/v2'
        d1_url = 'https://cn.dataone.org/cn/v2'
    elif env == 'staging':
        pasta_url = 'https://pasta-s.lternet.edu/package'
        gmn_url = f'https://gmn-s.{gmn_host}/mn/v2'
        d1_url = 'https://cn-stage.test.dataone.org/cn/v2'
    elif env == 'development':
        pasta_url = 'https://pasta-d.lternet.edu/package'
        gmn_url = None
        d1_url = None
    else:
        raise ValueError(f'Unknown environment "{env}"')

    if auth is not None:
        _ = auth.split(':')
        auth = (_[0], _[1])

    report = f'**** PASTA Data Package Report ****\n'

    success, response = get_resources(pid, pasta_url, auth)
    if success:
        resources = response.strip().split('\n')
        resources[-1] = get_package_doi(pid, pasta_url, auth)
        resource_metadata = get_resource_metadata(pid, pasta_url, auth)
        date_created = get_resource_create_date(resource_metadata)
        report += pasta_report(pid, resources, date_created)
        report += gmn_report(resources, gmn_url)
        report += cn_report(resources, d1_url)
    else:
        pid = '.'.join(pid)
        report += f'Package Identifier: {pid}\n'
        report += f'Status: {response}\n'

    click.echo(report)

    exit(0)


if __name__ == "__main__":
    track()

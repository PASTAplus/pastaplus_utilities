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

daiquiri.setup(level=logging.INFO,
               outputs=(
                   'stderr',
               ))
logger = daiquiri.getLogger('package_tracker: ' + __name__)


def get_d1_date_uploaded(sysmeta_xml: str) -> str:
    root = etree.fromstring(sysmeta_xml.encode('utf-8'))
    date_uploaded = root.find('.//dateUploaded')
    return date_uploaded.text


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


def pasta_report(pid: str, resources: list, date_created: str) -> str:
    indent = ' ' * 4
    pid = '.'.join(pid)
    report = f'Package Identifier: {pid}\n'
    report += f'Status: created {date_created}\n'
    report += f'DOI: {resources[-1]}\n'
    report += f'Resources:\n'
    for resource in resources[:-1]:
        report += f'{indent}{resource}\n'
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

    report = f'**** PASTA Data Package Provenance Report ****\n'

    success, response = get_resources(pid, pasta_url, auth)
    if success:
        resources = response.strip().split('\n')
        resources[-1] = get_package_doi(pid, pasta_url, auth)
        resource_metadata = get_resource_metadata(pid, pasta_url, auth)
        date_created = get_resource_create_date(resource_metadata)
        report += pasta_report(pid, resources, date_created)
        report += f'D1 GMN: {gmn_url}\n'
        report += f'GMN objects and upload time:\n'
        for resource in resources:
            success, response = get_d1_sysmeta(resource, gmn_url)
            if success:
                date_uploaded = get_d1_date_uploaded(response)
                report += f'    {resource} - {date_uploaded}\n'
            else:
                report += f'    {resource} - {response}\n'
        report += f'D1 CN: {d1_url}\n'
        report += f'CN objects and upload time:'
        for resource in resources:
            success, response = get_d1_sysmeta(resource, d1_url)
            if success:
                date_uploaded = get_d1_date_uploaded(response)
                report += f'    {resource} - {date_uploaded}\n'
            else:
                report += f'    {resource} - {response}\n'
    else:
        pid = '.'.join(pid)
        report += f'Package Identifier: {pid}\n'
        report += f'Status: {response}\n'

    click.echo(report)

    exit(0)


if __name__ == "__main__":
    track()

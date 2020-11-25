#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: download_counter

:Synopsis:

:Author:
    servilla

:Created:
    8/27/20
"""
import logging
import os
from typing import Set

import click
import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.exc import NoResultFound

import config

cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/download_counter.log"
daiquiri.setup(level=logging.INFO, outputs=(daiquiri.output.File(logfile), "stdout",))
logger = daiquiri.getLogger(__name__)

SQL_ENTITIES = (
    "SELECT resource_id FROM datapackagemanager.resource_registry "
    "JOIN most_recent_package_ids "
    "ON resource_registry.package_id=most_recent_package_ids.package_id "
    "WHERE datapackagemanager.resource_registry.resource_type='data' "
    "AND scope='<SCOPE>'"
)
SQL_COUNT = (
    "SELECT COUNT(*) FROM auditmanager.eventlog "
    "WHERE servicemethod='readDataEntity' AND statuscode=200 "
    "AND NOT userid='robot' AND resourceid='<RID>'"
)


def get_most_recent_entities(scope: str) -> Set:
    sql = SQL_ENTITIES.replace("<SCOPE>", scope)
    entities = query("package.lternet.edu:5432", sql)
    e = set()
    for entity in entities:
        e.add(entity[0])
    return e


def query(db_host: str, sql: str):
    rs = None
    db = (
        config.DB_DRIVER
        + "://"
        + config.DB_USER
        + ":"
        + config.DB_PW
        + "@"
        + db_host
        + "/"
        + config.DB_DB
    )
    engine = create_engine(db)
    try:
        with engine.connect() as connection:
            rs = connection.execute(sql).fetchall()
    except NoResultFound as e:
        logger.warning(e)
        rs = list()
    except Exception as e:
        logger.error(e)
        raise e
    return rs


start_help = (
    "Start date from which to begin search in ISO 8601 format"
    "(default is 20130101T00:00:00)"
)
end_help = (
    "End date from which to end search in ISO 8601 format"
    " (default is today)"
)


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("scope", nargs=1, required=True)
@click.option("-s", "--start", default="20130101T00:00:00", help=start_help)
@click.option("-e", "--end", default=None, help=end_help)
def main(scope: str, start: str, end: str):
    """
        Perform analysis of data entity downloads for given SCOPE from
        START_DATA to END_DATE.

        \b
            SCOPE: PASTA+ scope value
    """
    download_count = dict()
    entities = get_most_recent_entities(scope)
    for entity in entities:
        sql = SQL_COUNT.replace("<RID>", entity)
        count = query("audit.lternet.edu:5432", sql)
        download_count[entity] = count[0][0]
        print(f"{entity}, {count[0][0]}")

    return 0


if __name__ == "__main__":
    main()

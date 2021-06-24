#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: resource_mapper

:Synopsis:

:Author:
    servilla

:Created:
    6/24/21
"""
import asyncio
import logging
from typing import Optional
from pathlib import Path

import aiohttp
import click
import daiquiri
from lxml import etree


logfile = str(Path.cwd()) + Path(__file__).stem + ".log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.File(logfile),
        "stdout",
    ),
)
logger = daiquiri.getLogger(__name__)


async def get_resource_info(pid: str, pasta: str) -> Optional[dict]:
    pid_parts = pasta_identifier(pid)
    if pid_parts is None:
        msg = f"'{pid} is not in a recognized PASTA identifier format"
        raise ValueError(msg)
    else:
        scope, identifier, revision = pid_parts[0], pid_parts[1], pid_parts[2]

    url = f"https://{pasta}.lternet.edu/package/rmd/eml/{scope}/{identifier}/{revision}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            resource_xml = await resp.text()

    resource = dict()
    root = etree.fromstring(resource_xml.encode("UTF-8"))
    resource["date_created"] = root.find("dateCreated").text
    resource["doi"] = root.find("doi").text
    resource["identifier"] = root.find("identifier").text
    resource["package_id"] = root.find("packageId").text
    resource["principal_owner"] = root.find("principalOwner").text
    resource["resource_id"] = root.find("resourceId").text
    resource["resource_type"] = root.find("resourceType").text
    resource["revision"] = root.find("revision").text
    resource["scope"] = root.find("scope").text

    return resource


async def get_block(pids: list, pasta: str, csv: str, verbose: bool):
    tasks = []

    for pid in pids:
        tasks.append((pid, loop.create_task(get_resource_info(pid, pasta))))

    for pid, t in tasks:
        try:
            ri = await t
            if verbose or csv:
                row = f"{ri['package_id']},{ri['doi']},{ri['date_created']},\"{ri['principal_owner']}\""
                if verbose:
                    print(row)
                if csv:
                    with open(csv, "a+") as f:
                        f.write(row + "\n")
        except Exception as e:
            logger.error(e)


def is_pasta_identifier(pid: str) -> bool:
    is_pid = True
    if pasta_identifier(pid) is None:
        is_pid = False
    return is_pid


def pasta_identifier(pid: str) -> Optional[list]:
    pid_parts = pid.split(".")
    if len(pid_parts) != 3:
        return None
    else:
        try:
            int(pid_parts[1])
            int(pid_parts[2])
        except ValueError as e:
            logger.error(e)
            return None
    return pid_parts


csv_help = "Write output to CSV file"
block_size_help = "Number of concurrent requests to PASTA (default 5)"
env_help = (
    "PASTA environment (production, staging, development) to query (default production)"
)
verbose_help = "Display to stdout (default false)"
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("pid")
@click.option("-c", "--csv", default=None, help=csv_help)
@click.option("-b", "--block_size", default=5, help=block_size_help)
@click.option("-e", "--env", default="production", help=env_help)
@click.option("-v", "--verbose", is_flag=True, help=verbose_help)
def main(pid: str, csv: str, block_size: int, env: str, verbose: bool) -> int:

    if Path(pid).is_file():
        with open(pid) as f:
            pids = [_.strip() for _ in f.readlines()]
    elif is_pasta_identifier(pid):
        pids = [pid]
    else:
        msg = f"{pid} is neither a file or a PASTA identifier"
        logger.error(msg)
        return 1

    if env.lower() == "production":
        pasta = "pasta"
    elif env.lower() == "staging":
        pasta = "pasta-s"
    elif env.lower() == "development":
        pasta = "pasta-d"
    else:
        msg = f"'{env} not a recognized PASTA environment"
        logger.error(msg)
        return 1

    if verbose or csv:
        row = "package_id,doi,date_created,principal_owner"
        if verbose:
            print(row)
        if csv:
            with open(csv, "w+") as f:
                f.write(row + "\n")

    global loop
    loop = asyncio.get_event_loop()

    for i in range(0, len(pids), block_size):
        loop.run_until_complete(
            get_block(pids[i : i + block_size], pasta, csv, verbose)
        )

    return 0


if __name__ == "__main__":
    main()

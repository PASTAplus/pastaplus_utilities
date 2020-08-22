#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: entity_name_encoder

:Synopsis:

:Author:
    servilla

:Created:
    8/21/2020
"""
import logging
import hashlib
from pathlib import Path
from typing import List

import click
import daiquiri
from lxml import etree

daiquiri.setup(level=logging.INFO,
               outputs=(
                   'stderr',
               ))
logger = daiquiri.getLogger('package_tracker: ' + __name__)


def get_md5(entity_name: str) -> str:
    hashval = hashlib.md5(entity_name.encode())
    return hashval.hexdigest()


def get_entity_names(datasets) -> List:
    names = list()
    for dataset in datasets:
        if dataset is not None:
            entity_name = dataset.find(".//entityName")
            names.append(entity_name.text.strip())
    return names


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('eml', nargs=1, required=True)
def main(eml: str):
    """
        Encodes entity names in the EML file to the PASTA equivalent.

        \b
            EML: EML file
    """
    p = Path(eml)
    if not p.exists():
        msg = f"File '{eml}' not found."
        raise FileNotFoundError(msg)

    eml_file = p.read_text()

    names = list()
    xml = etree.fromstring(eml_file.encode("utf"))
    datatables = xml.findall("./dataset/dataTable")
    names += get_entity_names(datatables)
    spatialrasters = xml.findall("./dataset/spatialRaster")
    names += get_entity_names(spatialrasters)
    spatialvectors = xml.findall("./dataset/spatialVector")
    names += get_entity_names(spatialvectors)
    otherentities = xml.findall("./dataset/otherEntity")
    names += get_entity_names(otherentities)

    for name in names:
        md5 = get_md5(name)
        print(f"{md5} - {name}")

if __name__ == "__main__":
    main()
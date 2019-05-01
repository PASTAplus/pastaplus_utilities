#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: project

:Synopsis:

:Author:
    servilla

:Created:
    2/22/19
"""
from pathlib import Path

import click
import daiquiri
from lxml import etree


logger = daiquiri.getLogger('project: ' + __name__)


def flatten(element):
    t = ''
    if hasattr(element, 'text') and element.text is not None:
        t = element.text.strip().replace('\t', '').replace('\n', '').replace('\r', '')
    if hasattr(element, '__iter__'):
        for e in element:
            t += flatten(e)
    return t


@click.command()
@click.argument('e_dir')
def main(e_dir: str):

    if not Path(e_dir).is_dir():
        logger.error(f'Directory "{e_dir}" does not exist')
        exit(1)

    for eml_f in Path(e_dir).iterdir():
        with open(str(eml_f), 'r') as f:
            eml = f.read()
            root = etree.fromstring(eml.encode("utf-8"))
            funding = root.find('.//project/funding')
            if funding is not None:
                pid = root.get('packageId')
                t = flatten(funding)
                print(f'{pid}\t{t}')

    return 0


if __name__ == "__main__":
    main()

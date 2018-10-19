#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: generate_standard_units

:Synopsis:
 
:Author:
    Duane Costa

:Created:
    10/19/18
"""

from docopt import docopt
import logging
import sys
import xml.etree.ElementTree as ET

logging.basicConfig(format='%(asctime)s %(levelname)s (%(name)s): %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S%z',
                    # filename='offline_data' + '.log',
                    level=logging.INFO)

logger = logging.getLogger('offline_data')


def generate_standard_units(fin, fout=sys.stdout):
    """
    Generates an alphabetically sorted list of standard units from the
    EML 2.2 file eml-unitDictionary.xml. Units with a deprecatedInFavorOf
    attribute are excluded from the list.

    Also generates an alphabetically sorted list of deprecatedInFavorOf units,
    though these are probably of less interest.
    """
    standard_units = []
    deprecated_units = []

    try:
        tree = ET.parse(fin)
        root = tree.getroot()

        for child in root:
            if child.tag[-4:] == 'unit':
                unit_name = child.get('name')
                deprecatedInFavorOf = child.get('deprecatedInFavorOf')
                if not deprecatedInFavorOf:
                    standard_units.append(unit_name)
                else:
                    deprecated_units.append(unit_name)

        print("STANDARD UNITS", file=fout)
        standard_units.sort(key=lambda s: s.lower())
        print(standard_units, file=fout)
        print('', file=fout)
        print('DEPRECATED UNITS', file=fout)
        deprecated_units.sort(key=lambda s: s.lower())
        print(deprecated_units, file=fout)
    except Exception as e:
        logger.error(e)


def main():
    """
    Reports on PASTA data packages with offline data entities.

    Usage:
        generate_standard_units.py [-i | --input <input>]
        generate_standard_units.py [-o | --output <output>]
        generate_standard_units.py [-h | --help]

    Options:
        -i --input   XML input file (usually eml-unitDictionary.xml)
        -o --output   Output results to file; 
        -h --help     This page

    """
    args = docopt(str(main.__doc__))

    input_file = args['<input>']
    output = args['<output>']

    if input_file is None:
        input_file = 'eml-unitDictionary.xml'

    fin = open(input_file, 'rt')

    if output is None:
        fout = sys.stdout
    else:
        fout = open(output, 'w')

    generate_standard_units(fin=fin, fout=fout)

    if fin:
        fin.close()

    if output:
        fout.close()
        

if __name__ == "__main__":
    main()

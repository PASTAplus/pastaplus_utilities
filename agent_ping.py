#!/usr/bin/env python
# -*- coding: utf-8 -*-

""":Mod: agent_ping

:Synopsis:

:Author:
    servilla

:Created:
    10/19/18
"""
import logging

import daiquiri
from docopt import docopt
import requests


log_file = __file__.strip('.py')
daiquiri.setup(level=logging.INFO,
               outputs=(
                   'stderr',
                   # daiquiri.output.File(filename=f'{log_file}.log'),
                ))

logger = daiquiri.getLogger('agent_ping: ' + __name__)

def ping(url: str, agent: str, verbose: bool = False):
    headers = {'User-Agent': agent}
    r = requests.get(url, headers=headers)

    if verbose:
        print(f'Status: {r.status_code}')
        for h in r.headers:
            print(f'{h}: {r.headers[h]}')


def main():
    """
    Sends simple HTTP(S) requests to a web server with new user agent
    field for testing bot filtering.

    Usage:
        agent_ping.py <url> [-s | --string <agent_string>]
                      [-f | --file <agent_string_file>]
                      [-v | --verbose]

    Arguments:
        url         The web server URL to be pinged


    Options:
        -h --help       This page
        -s --string     The user agent string
        -f --file       The file contain user agent strings (one per line)
        -v --verbose   Provide a verbose response, including headers
    """

    args = docopt(str(main.__doc__))

    url = args['<url>']

    agent_strings = list()
    if args['--string']:
        agent_strings.append(args['<agent_string>'].strip())
    elif args['--file']:
        agent_string_file = args['<agent_string_file>']
        with open(agent_string_file, 'r') as f:
            agent_strings = [_.strip() for _ in f.readlines()]
    else:
        msg = f'No user agent string or file provided!'
        logger.warning(msg)

    verbose = False
    if args['--verbose']:
        verbose = True

    for agent_string in agent_strings:
        ping(url, agent_string, verbose=True)

    return 0


if __name__ == "__main__":
    main()

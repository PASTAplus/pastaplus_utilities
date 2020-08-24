#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
:Mod: offline_checksum

:Synopsis:
    Perform checksum analysis of offline data files

:Author:
    servilla

:Created:
    8/23/20
"""
import logging
import hashlib
import os
from pathlib import Path

import click
import daiquiri


cwd = os.path.dirname(os.path.realpath(__file__))
logfile = cwd + "/offline_checksum.log"
daiquiri.setup(level=logging.DEBUG,
               outputs=(daiquiri.output.File(logfile), "stdout",))
logger = daiquiri.getLogger(__name__)


def get_files(data: Path, ext: str = ""):
    f = list()
    if len(ext) > 0:
        ext = "." + ext.lstrip(".")
    files = data.rglob(f"*{ext}")
    for file in files:
        if Path(file).is_file():
            f.append(file)
    return f


def do_report(report: str, results: dict):
    if report is None:
        for file, checksum in results.items():
            result = f"{file},{checksum}"
            print(result)
    else:
        with open(report, "w") as r:
            for file, checksum in results.items():
                result = f"{file},{checksum}\n"
                r.write(result)


report_help = "Report file (defaults to stdout only)"
manifest_help = "Import a manifest of prior checksums for comparison"
verbose_help = "Print progress to stdout"
md5_help = "Perform MD5 checksum analysis only"
sha1_help = "Perform SHA1 checksum analysis only"
ext_help = "Data file extension (default is none)"
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("data",  nargs=1, required=True)
@click.option("-r", "--report", default=None, help=report_help)
@click.option("-m", "--manifest", default=None, help=manifest_help)
@click.option("-v", "--verbose", is_flag=True, default=False, help=verbose_help)
@click.option("--md5", is_flag=True, help=md5_help)
@click.option("--sha1", is_flag=True, help=sha1_help)
@click.option("--ext", default="", help=ext_help)
def main(
        data: str,
        report: str,
        manifest: str,
        verbose: bool,
        md5: bool,
        sha1: bool,
        ext: str
):
    """
        Perform checksum analysis of offline data files. By default, both
        MD5 and SHA1 checksum analysis are done per file.

        \b
            DATA: Data directory where checksum analysis begins.
    """
    d = Path(data)
    if not (d.exists() and d.is_dir()):
        msg = f"Data directory '{data}' was not found or is not a directory"
        raise FileNotFoundError(msg)

    if report is not None:
        r = Path(report)
        if not (r.parent.exists() and r.parent.is_dir()):
            msg = f"Report '{report}' path is not a valid path"
            raise FileNotFoundError(msg)

    if manifest is not None:
        m = Path(manifest)
        if not (m.exists() and m.is_file()):
            msg = f"Manifest '{manifest}' was not found or is not a file"
            raise FileNotFoundError(msg)

    if not (md5 or sha1):
        msg = "Either an MD5 or SHA1 hash algorithm must be selected"
        raise ValueError(msg)
    elif md5 and sha1:
        msg = "Only one of MD5 or SHA1 hash algorithms should be selected"
        raise ValueError(msg)
    if md5:
        hash_algorithm = hashlib.md5
    else:
        hash_algorithm = hashlib.sha1

    files = get_files(d, ext)
    results = dict()
    for index, file in enumerate(files, start=1):
        file = str(file)
        checksum = hash_algorithm(open(file, "rb").read()).hexdigest()
        results[file] = checksum if (manifest is None) else checksum + ","
        if verbose:
            print(f"{index}: {str(file)} - {checksum}")

    if manifest is None:
        do_report(report, results)
    else:
        with open(manifest, "r") as m:
            lines = m.readlines()
            for line in lines:
                m_file, m_checksum = line.split(",")
                if m_file in results:
                    checksum = m_checksum.strip()
                    if checksum + "," != results[m_file]:
                        results[m_file] = results[m_file] + "fail"
                        msg = f"Checksum mismatch - {results[m_file]}"
                        logger.warning(msg)
                    else:
                        results[m_file] = results[m_file] + "pass"
                else:
                    msg = f"Manifest `{m_file}` not found in data directory"
                    logger.warning(msg)
        do_report(report, results)

    return 0


if __name__ == "__main__":
    main()

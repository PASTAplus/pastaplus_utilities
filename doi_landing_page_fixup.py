#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Get all EDI-provided DOIs from DataCite and, for DOIs with a landing page URL that points to the
 LTER data portal, modify the URL to point to the EDI data portal.

 Saves the DOIs and their data dictionaries to a pickle file. If that file is present, its contents
 are used and we don't go to DataCite for the DOIs. This makes it possible to test fixup without
 having to wait to get the DOIs from DataCite. This is ok when the DOI list is stable (i.e., we've
 stopped PASTA from creating DOIs that point to the LTER portal, so that no new DOIs will be created
 that need to be handled). To get fresh results from DataCite, delete the pickle file before running.
"""

import click
import json
import pickle
import requests

import datacite_config


@click.command()
@click.option(
    "-e",
    default=None,
    help="Filepath for text file containing list of DOIs to exclude from processing."
)
def doi_landing_page_fixup(e: str):
    """
    Get all EDI-provided DOIs from DataCite and, for DOIs with a landing page URL that points to the
     LTER data portal, modify the URL to point to the EDI data portal. Saves the DOIs and their data
     dictionaries in pickle file datacite.pkl. If that file is present, its contents are used and we
     don't query DataCite for the DOIs.
    """
    main(e)


doi_data = {}
exclude = []
filtered = {}
skipped = {}


def get_doi_data():
    """
    Query the DataCite REST API to get the complete list of EDI-provided DOIs and save their data dictionaries.
    """

    def parse_doi_response(data_dict):
        for data in data_dict['data']:
            id = data['id']
            doi_data[id] = data

    # Get a page of responses from DataCite and parse them. Each response (except the last)
    #  contains the URL to use to query for the next page of responses.
    def get_response(url):
        next_url = None
        response = requests.request("GET", url, headers=headers)
        if response:
            data_dict = json.loads(response.text)
            parse_doi_response(data_dict)
            if 'next' in data_dict['links']:
                next_url = data_dict['links']['next']
        else:
            print(f'ERROR: requests error {response.status_code} for GET {url}')
        return next_url

    headers = {'accept': 'application/vnd.api+json'}
    url = f"https://api.{datacite_config.DATACITE_DOMAIN}/providers/edi/dois?page[cursor]=1&page[size]=1000"
    next_url = get_response(url)

    while next_url:
        next_url = get_response(next_url)


def filter_data():
    """
    Filter the data, as desired. This is broken out as a separate pass through the data to make
     it easier to adapt this script to a different use case.
    """
    for id in doi_data:
        data = doi_data[id]
        url = data['attributes']['url']
        if 'lternet.edu' in url:
            filtered[id] = url
        else:
            skipped[id] = url
    print(f'{len(filtered)} landing page URLs will be modified, {len(skipped)} are OK as is\n')


def fixup_landing_pages(to_do, exclude_list):
    """
    Go thru all of the DOIs whose landing page URLs point to the LTER data portal and modify
     their landing page URLs to point to the EDI data portal.
    """

    for id, url in to_do.items():
        if id in exclude_list:
            continue
        url = url.replace('lternet.edu', 'edirepository.org')
        update_url = f"https://api.{datacite_config.DATACITE_DOMAIN}/dois/{id}"
        headers = {'content-type': "application/vnd.api+json"}
        payload = f'{{"data": {{"id": "{id}", "attributes": {{"url": "{url}"}}}}}}'
        response = requests.request("PUT", update_url, auth=(datacite_config.USERNAME, datacite_config.PASSWORD),
                                    headers=headers, data=payload)

        if response:
            # print the DOIs we've done, so they can be captured and excluded on subsequent runs
            print(f'{id}')
        else:
            print(f'ERROR: requests error {response.status_code} {response.text} for PUT {update_url} with payload {payload}')


def main(exclude_filename: str):
    global doi_data

    # Take the DOI data from the pickle file, if present. Otherwise, go get it from DataCite and save to pickle file.
    try:
        fp = open(datacite_config.PICKLE_FILE, 'rb')
        doi_data = pickle.load(file=fp)
    except FileNotFoundError:
        get_doi_data()
        fp = open(datacite_config.PICKLE_FILE, 'wb')
        pickle.dump(doi_data, fp)

    # If a file was provided of cases to exclude, read it in
    if exclude_filename:
        lines = []
        try:
            with open(exclude_filename, 'r') as exclude_file:
                lines = exclude_file.readlines()
        except FileNotFoundError:
            pass
        else:
            for line in lines:
                exclude.append(line.rstrip('\n'))

    # Make a pass through the data to pull out the cases we want to act on.
    filter_data()

    # Act on the filtered cases.
    if datacite_config.MODIFY_LANDING_PAGE_URLS:
        fixup_landing_pages(filtered, exclude)


if __name__ == '__main__':
    doi_landing_page_fixup()

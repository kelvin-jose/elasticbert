"""

Actual indexing is done here.

"""

import json
from argparse import ArgumentParser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def load_dataset(path):
    with open(path) as f:
        return [json.loads(line) for line in f]


def main(args):
    client = Elasticsearch('localhost:9200')
    docs = load_dataset(args.data)
    bulk(client, docs)


if __name__ == '__main__':
    parser = ArgumentParser(description='indexing ES documents.')
    parser.add_argument('--data', help='ES documents.')
    args = parser.parse_args()
    main(args)
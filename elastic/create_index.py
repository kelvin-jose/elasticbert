"""

This file creates an index in the elasticsearch from a config file.

"""

from json import load
from argparse import ArgumentParser
from elasticsearch import Elasticsearch

es = Elasticsearch('localhost:9200')

def create_index(index, config):
    try:
        with open(config) as file:
            config = load(file)

        es.indices.create(index=index, body=config)
        print("[INFO] index " + index + " has been created!")
    except Exception as e:
        print("[WARNING] some exception has occurred!")


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('--index', required=True, help='name of the ES index')
    parser.add_argument('--config', required=True, help='path to the ES mapping config')
    args = parser.parse_args()
    create_index(args.index, args.config)
"""

This script creates documents in the required format for
indexing.

"""

import json
from pandas import read_csv
from argparse import ArgumentParser
from bert_serving.client import BertClient
bc = BertClient(output_fmt='list', check_length=False)


def create_document(doc, emb, index_name):
    return {
        '_op_type': 'index',
        '_index': index_name,
        'title': doc['title'],
        'abstract': doc['abstract'],
        'abstract_vector': emb
    }


def load_dataset(path):
    docs = []
    df = read_csv(path)
    for row in df.iterrows():
        series = row[1]
        doc = {
            'title': series.title,
            'abstract': series.abstract
        }
        docs.append(doc)
    return docs


def bulk_predict(docs, batch_size=256):
    for i in range(0, len(docs), batch_size):
        batch_docs = docs[i: i+batch_size]
        embeddings = bc.encode([doc['abstract'] for doc in batch_docs])
        for emb in embeddings:
            yield emb


def main(args):
    docs = load_dataset(args.csv)
    with open(args.output, 'w') as f:
        for doc, emb in zip(docs, bulk_predict(docs)):
            d = create_document(doc, emb, args.index)
            f.write(json.dumps(d) + '\n')

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('--index', required=True, help='name of the ES index')
    parser.add_argument('--csv', required=True, help='path to the input csv file')
    parser.add_argument('--output', required=True, help='name of the output file (example.json1)')
    args = parser.parse_args()
    main(args)
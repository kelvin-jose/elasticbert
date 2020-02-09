from elasticsearch import Elasticsearch
from bert_serving.client import BertClient
from elasticsearch.exceptions import ConnectionError, NotFoundError

# total number of responses
SEARCH_SIZE = 1

# establishing connections
bc = BertClient(ip='localhost', output_fmt='list', check_length=False)
client = Elasticsearch('localhost:9200')

# this query is used as the search term, feel free to change
query = 'machine learning'
query_vector = bc.encode([query])[0]

script_query = {
    "script_score": {
        "query": {"match_all": {}},
        "script": {
            "source": "cosineSimilarity(params.query_vector, doc['abstract_vector']) + 1.0",
            "params": {"query_vector": query_vector}
        }
    }
}

try:
    response = client.search(
         index='researchgate',  # name of the index
         body={
             "size": SEARCH_SIZE,
             "query": script_query,
             "_source": {"includes": ["title", "abstract"]}
         }
     )
    print(response)
except ConnectionError:
    print("[WARNING] docker isn't up and running!")
except NotFoundError:
    print("[WARNING] no such index!")

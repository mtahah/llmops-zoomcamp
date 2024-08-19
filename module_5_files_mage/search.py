from elasticsearch import Elasticsearch

es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

query = "When is the next cohort?"

#indices = es.indices.get_alias("*")

response = es.cat.indices(index="*", format="json")
indices = [index["index"] for index in response]

for index in indices:
    search_body = {
        "query": {
            "match": {
                "question": query
            }
        }
    }

    try:
        response = es.search(index=index, body=search_body)

        if response['hits']['total']['value'] > 0:
            print(f"Found {response['hits']['total']['value']} hits in index {index}")
            for hit in response['hits']['hits']:
                print(hit['_source'])
            print('\n\n\n')
        else:
            print(f"No hits found in index {index}")
        print('\n\n\n')
    except Exception as e:
        print(f"Error searching index {index}: {str(e)}")
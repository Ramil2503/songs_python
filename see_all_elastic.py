from elasticsearch import Elasticsearch

# Assuming Elasticsearch client is already initialized
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

def get_all_documents(index_name="songs"):
    """Retrieve and print all documents in the specified index with their IDs and index names."""
    try:
        response = es.search(index=index_name, body={"query": {"match_all": {}}})
        documents = response['hits']['hits']
        for doc in documents:
            print(f"ID: {doc['_id']}, Index: {doc['_index']}, Content: {doc['_source']}")
    except Exception as e:
        print(f"Error retrieving documents: {e}")

# Call this function to check documents in Elasticsearch
get_all_documents()

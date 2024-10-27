from elasticsearch import Elasticsearch

# Assuming Elasticsearch client is already initialized
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

def delete_document(index_name, doc_id):
    """Delete a document by ID."""
    try:
        es.delete(index=index_name, id=doc_id)
        print(f"Deleted document with ID: {doc_id}")
    except Exception as e:
        print(f"Error deleting document with ID {doc_id}: {e}")

# Get the index name and document IDs from user input
index_name = "songs"
first_doc_id = input("Enter the document ID to delete: ")

# Delete the documents
delete_document(index_name, first_doc_id)

import sys

import dealroom_firestore_connector as fc

# Connect to staging Firestore
db = fc.new_connection(project="sustained-hold-288413", credentials_path="cred.json",)
if db == -1:
    # Error. Do something else
    sys.exit()

# Go to desired collection
collection_ref = db.collection(u"firestore_connector_test")

# Create a document
doc_ref = collection_ref.document(u"test_file")
if fc.set(doc_ref, {u"website": u"abc.com", u"index": u"123123"}) == -1:
    # Error. Do something else (i.e. continue to next doc)
    sys.exit()
else:
    pass


# Get a document
doc_ref = collection_ref.document(u"test_file")
doc = fc.get(doc_ref)
if doc != -1:
    print(doc.id)
    print(doc.to_dict())

# Update a document
if fc.update(doc_ref, {u"website": u"123.com"}) != -1:
    pass  # Continue if no error in update call
else:
    sys.exit()

# Stream a collection
stream = fc.stream(collection_ref)
if stream != -1:
    for doc in stream:
        print(doc.id)
        print(doc.to_dict())
else:
    sys.exit()


## Batching
batch = fc.Batcher(db)
# -----
# Option 1: Sequentially
batch.set(collection_ref.document("doc1"), {"foo1": "bar1"})
batch.set(collection_ref.document("doc2"), {"foo2": "bar2"})
batch.update(collection_ref.document("doc3"), {"foo3": "bar3"})

# Get total writes in the batch so far
print(batch.total_writes)  # Outputs: 3

status = batch.commit()
if status < 0:
    print("Failed to commit changes")
else:
    print("Successful commited changes")


# -----
# Option 2: Using context manager pattern
with fc.Batcher(db) as batch:
    batch.set(collection_ref.document("doc1"), {"foo1": "bar1"})
    batch.set(collection_ref.document("doc2"), {"foo2": "bar2"})
    batch.update(collection_ref.document("doc3"), {"foo3": "bar3"})

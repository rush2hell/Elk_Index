from PIL import Image
import imagehash
import cv2 
import os 
import glob
from elasticsearch import Elasticsearch
import datetime

#connection with elasticsearch
# create a client instance of Elasticsearch
es = Elasticsearch([{'host': '84.247.12.226', 'port': 9200}])

#creating index
#es.indices.create(index="images")

# Looking if the index exists
check = es.indices.exists(index="images")

img_dir = "images/"
data_path = os.path.join(img_dir,'*g') 
files = glob.glob(data_path) 
data = []
hash_doc = {"caption":[], "hash":[], "timestamp": str(datetime.datetime.now())};
_id = 1
for f1 in files: 
    img = cv2.imread(f1) 
    data.append(img)
    #print(f1)
    dhash = imagehash.average_hash(Image.open(f1))
    hash_value = str(dhash)
    hash_doc["caption"].append(f1)
    hash_doc["hash"].append(hash_value)
    print("Value for ID", _id)
    es.index(index="images", doc_type="_doc", id=_id, body=hash_doc)
    _id = _id + 1
    #print(dhash)
print("Final Dict: ",hash_doc)
print("-----------Data from ELK Server----------")
for i in range(1,_id):
    res = es.get(index="images", doc_type="_doc", id=i)
    print(res)
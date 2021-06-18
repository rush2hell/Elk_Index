import cv2 
import os 
import glob 
# import the Elasticsearch low-level client
from elasticsearch import Elasticsearch
import numpy as np
# import the Image and TAGS classes from Pillow (PIL)
from PIL import Image
from PIL.ExifTags import TAGS

import uuid # for image meta data ID
import base64 # convert image to b64 for indexing
import datetime # for image meta data timestamp

# create a client instance of Elasticsearch
elastic_client = Elasticsearch([{'host': '84.247.12.226', 'port': 9200}])

"""
Function that uses PIL's TAGS class to get an image's EXIF
meta data and returns it all in a dict
"""
def get_image_exif(img):
    # use PIL to verify image is not corrupted
    img.verify()

    try:
        # call the img's getexif() method and return EXIF data
        exif = img._getexif()
        exif_data = {}

        # iterate over the exif items
        for (meta, value) in exif.items():
            try:
                # put the exif data into the dict obj
                exif_data[TAGS.get(meta)] = value
            except AttributeError as error:
                print ('get_image_meta AttributeError for:', file_name, '--', error)
    except AttributeError:
        # if img file doesn't have _getexif, then give empty dict
        exif_data = {}
    return exif_data

"""
Function to create new meta data for the Elasticsearch
document. If certain meta data is missing from the orginal,
then this script will "fill in the gaps" for the new documents
to be indexed.
"""
def create_exif_data(img):

    # create a new dict obj for the Elasticsearch doc
    es_doc = {}
    es_doc["size"] = img.size

    # put PIL Image conversion in a try-except indent block
    try:
        # create PIL Image from path and file name
        img = Image.open(_file)
    except Exception as error:
        print ('create_exif_data PIL ERROR:', error, '-- for file:', _file)

    # call the method to have PIL return exif data
    exif_data = get_image_exif(img)

    # get the PIL img's format and MIME
    es_doc["image_format"] = img.format
    es_doc["image_mime"] = Image.MIME[img.format]

    # get datetime meta data from one of these keys if possible
    if 'DateTimeOriginal' in exif_data:
        es_doc['datetime'] = exif_data['DateTimeOriginal']

    elif 'DateTime' in exif_data:
        es_doc['datetime'] = exif_data['DateTime']

    elif 'DateTimeDigitized' in exif_data:
        es_doc['datetime'] = exif_data['DateTimeDigitized']

    # if none of these exist, then use current timestamp
    else:
        es_doc['datetime'] = str( datetime.datetime.now() )

    # create a UUID for the image if none exists
    if 'ImageUniqueID' in exif_data:
        es_doc['uuid'] = exif_data['ImageUniqueID']
    else:
        # create a UUID converted to string
        es_doc['uuid'] = str( uuid.uuid4() )

    # make and model of the camera that took the image
    if 'Make' in exif_data:
        es_doc['make'] = exif_data['Make']
    else:
        es_doc['make'] = "Camera Unknown"

    # camera unknown if none exists
    if 'Model' in exif_data:
        es_doc['model'] = exif_data['Model']
    else:
        es_doc['model'] = "Camera Unknown"

    if 'Software' in exif_data:
         es_doc['software'] = exif_data['Software']
    else:
         es_doc['software'] = exif_data['Unknown Software']

    # get the X and Y res of image
    if 'XResolution' in exif_data:
        es_doc['x_res'] = exif_data['XResolution']
    else:
        es_doc['x_res'] = None

    if 'YResolution' in exif_data:
        es_doc['y_res'] = exif_data['YResolution']
    else:
        es_doc['y_res'] = None
    # return the dict
    return es_doc

img_dir = "test/"
data_path = os.path.join(img_dir,'*g') 
files = glob.glob(data_path) 
data = [] 
for f1 in files: 
    img = cv2.imread(f1) 
    data.append(img)
    print(f1) 
    # create an Image instance of photo
    _file = f1
    _index = "images"
    _id = 1
    img = Image.open(open(_file, 'rb'))
    print(img)
    # get the _source dict for Elasticsearch doc
    _source = create_exif_data(img)

    # store the file name in the Elasticsearch index
    _source['name'] = _file

    # covert NumPy of PIL image to simple Python list obj
    img_array = np.asarray( Image.open( _file ) ).tolist()

    # convert the nested Python array to a str
    img_str = str(img_array)

    # put the encoded string into the _source dict
    _source["raw_data"] = img_str

    # create the "images" index for Elasticsearch if necessary
    resp = elastic_client.indices.create(
        index = _index,
        body = "{}",
        ignore = 400 # ignore 400 already exists code
    )

    print ("\nElasticsearch create() index response -->", resp)

    # call the Elasticsearch client's index() method
    resp = elastic_client.index(
        index = _index,
        doc_type = '_doc',
        id = _id,
        body = _source,
        request_timeout=160
    )
    print ("\nElasticsearch index() response -->", resp)
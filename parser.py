"""
Nota:
Este programa foi adaptado de 
https://github.com/chezou/amazon-movie-review/blob/master/src/preparation/parse-amazon-meta.py
"""

import re
import gzip
from tqdm import tqdm

def rename_review_key(entry):
  if(b'reviews' in entry and type(entry[b'reviews']) == type('')):
    temp = entry[b'reviews']
    del entry[b'reviews']
    entry['review_stats'] = temp
  return entry

def parse(filename, total):
  IGNORE_FIELDS = ['Total items', 'reviews']
  with gzip.open(filename, 'rt') as f:
        entry = {}
        categories = []
        reviews = []
        similar_items = []

        for line in tqdm(f, total=total):
            line = line.strip()
            colonPos = line.find(':')

            if line.startswith("Id"):
                if reviews:
                    entry["reviews"] = reviews
                if categories:
                    entry["categories"] = categories
                yield entry
                entry = {}
                categories = []
                reviews = []
                rest = line[colonPos+2:]
                entry["id"] = '' + rest.strip()

            elif line.startswith("similar"):
                similar_items = line.split()[2:]
                entry['similar_items'] = similar_items

            # "cutomer" is typo of "customer" in original data
            elif line.find("cutomer:") != -1:
                review_info = line.split()
                reviews.append({'_date': review_info[0],
                                'customer_id': review_info[2],
                                'rating': int(review_info[4]),
                                'votes': int(review_info[6]),
                                'helpful': int(review_info[8])})

            elif line.startswith("|"):
                categories.append(line)

            elif colonPos != -1:
                eName = line[:colonPos]
                rest = line[colonPos+2:]

                if not eName in IGNORE_FIELDS:
                    entry[eName] = '' + rest.strip()

        if reviews:
            entry["reviews"] = reviews
        if categories:
            entry["categories"] = categories

        yield entry



if __name__ == '__main__':
  file_path = "amazon-meta.txt.gz"

  import simplejson

  for e in parse(file_path, 15010574):
    if e:
      print(simplejson.dumps(e))


from elasticsearch import Elasticsearch
import json
import yaml

def yaml_loader(yaml_file):
	with open(yaml_file) as yml:
		config = yaml.load(yml)
	return config

config_path = 'config/stream_consumer_config.yml'
config = yaml_loader(config_path)
es = Elasticsearch(hosts=[{'host': config['nameanodeip'], 'port':9200}], http_auth=('elastic','changeme'))
es.indices.refresh(index="cars")

query = { "query": { "filtered" : {"query" : {"match_all" : {}},
        "filter":
          { "geo_distance": {
            "distance":      "10 miles",
            "distance_type": "sloppy_arc",
            "pick_location": {
              "lat":   40.748609999999999,
              "lon": -73.984409999999983}
        }
      }
    }
  }
}

result = es.search(index='cars',body=query, ignore = 400,size=3)

print json.dumps(result, indent=2)

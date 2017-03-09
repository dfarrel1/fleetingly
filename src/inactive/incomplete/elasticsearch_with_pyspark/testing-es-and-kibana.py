map = {"users":{
    "mappings":{
      "user":{
        "_source": { "enabled": True },
        "properties": {
          "pick_location": {"type": "geo_point","lat_lon": True},
          "drop_location": {"type": "geo_point","lat_lon": True},
          "timestamp":     {"type": "date_hour_minute_second_millis",    "doc_values": True, "format": "yyyy-MM-dd'T'HH:mm:ss.SSS"}
            }
          }
      }
    }
    }
doc = {
    'text': 'heliotrope halcyon amarinthine.',
    'pick_location':  {'lat': 40, 'lon': -74},
    'drop_location': {'lat': 40, 'lon': -74},
    '@timestamp': datetime.now().strftime("%Y-%m-%d'T'%H:%M:%S.%f")[:-3],
}
es.indices.delete(index='testmaps',ignore=[400, 404])         
es.indices.create(index='testmaps', ignore=400, body=map)
res = es.index(index='testmaps', doc_type='users', id=1, body=doc)
print(res['created'])
res = es.get(index='testmaps', doc_type='users', id=1)
print(res['_source'])
curl --user elastic:changeme -XPUT 'http://localhost:9200/realtime2' -d '
{
  "mappings": {
    "user": {
      "properties": {
        "cluster": {
          "type": "integer"
        },
        "location": {
          "type": "geo_point"
        },
       "time_stamp":    { "type": "date", "format": "yyyy-MM-dd"'T'"HH:mm:ss.SSS"}
        }
      }
    }
  }
}
'



"format": "yyyy-MM-dd HH:mm:ss.SSS" 
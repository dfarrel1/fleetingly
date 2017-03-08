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
       "time_stamp":    { "type": "date", "format": "date_hour_minute_second_millis"}
        }
      },
    "car": {
        "properties": {
            "cluster": {
                "type": "integer"
            },
            "location": {
                "type": "geo_point"
            },
            "time_stamp":    { "type": "date", "format": "date_hour_minute_second_millis"}
            }
        }
    }
}
'




curl --user elastic:changeme -XPUT 'http://localhost:9200/clusters' -d '
{
    "mappings": {
        "cluster": {
            "properties": {
                "cluster": {
                    "type": "integer"
                },
                "location": {
                    "type": "geo_point"
                },
                "ratio":    { "type": "double"}
            }
        }
    }
}
'

##curl -XDELETE --user elastic:changeme 'localhost:9200/realtime2'

curl -XGET --user elastic:changeme 'localhost:9200/_cat/indices?v'
curl --user elastic:changeme -XGET 'http://localhost:9200/realtime2/_mapping/'



# #(parse err): "yyyy-MM-dd"'T'"HH:mm:ss.SSS"
#"format": "yyyy-MM-dd HH:mm:ss.SSS"

### ES SETTINGS FILES
curl "localhost:9200/_nodes/settings?pretty=true"

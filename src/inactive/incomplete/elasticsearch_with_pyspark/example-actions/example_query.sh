curl --user elastic:changeme  -XGET 'localhost:9200/_search?pretty' -H 'Content-Type: application/json' -d'
{
    "query": {
        "template": {
            "inline": { "match": { "text": "{{query_string}}" }},
            "params" : {
                "query_string" : "all about search"
            }
        }
    }
}
'


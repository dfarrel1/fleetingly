### Scripting Query
curl --user elastic:changeme -XGET 'localhost:9200/_search?pretty' -H 'Content-Type: application/json' -d'
{
    "query": {
        "bool" : {
            "must" : {
                "script" : {
                    "script" : {
                        "inline": "doc['num1'].value > 1",
                        "lang": "painless"
                     }
                }
            }
        }
    }
}
'


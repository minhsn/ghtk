Bài 1:
      
GET dantri/_search
{
 "query": {
   "bool": {
      "must": [
        {  
          "range": {
            "time": {
              "gte": 1356998400,
              "lte": 1388534400
            }
          }
        }
        ,
        {
           "multi_match": {
            "query": "an toàn, đường sắt,đường bộ",
            "fields": ["title","description","content"]
          }
        }
      ]
   }    
 }
}

Bài 2:
PUT dantri
{
"mappings": {
   "properties": {
       "title": {
           "type": "keyword"
            }
        }
     }
  }

 GET dantri\_search
{
  "query": {
    "bool": {
      "must" : {
        "match_phrase_prefix" : {
          "title" : "Hà Nội"
        }
      },
      "must_not": [
        {
            "match": {
            "description": "Hà Nội"
            }
        }
      ]
    }
  },
    "sort" : {
    "time" : { "order" : "desc" }
  }
} 

bài 3
hàm main.py dùng để sinh lấy title sau đó tính tfidf của các từ có trong đó 
dữ liệu khi đẩy vào elastic search có dạng sau
{"index":{"_index":"dantri","_id":29214}}
{"suggest_title": [{"input" : "Tàu ngầm" , "weight" : 14},{"input" : "Kilo" , "weight" : 6},{"input" : "Đà Nẵng" , "weight" : 519},{"input" : "đã" , "weight" : 434},{"input" : "về" , "weight" : 1115},{"input" : "đến" , "weight" : 332},{"input" : "quân cảng" , "weight" : 2},{"input" : "Cam Ranh" , "weight" : 32}]}

PUT dantri
{
  "mappings": {
    "properties": {
      "suggest_title": {
        "type": "completion" 
      }
    }
  }
}

file GetTitle dùng để nhập từ cần tìm cả từ không dấu trả về tập các từ gợi ý 

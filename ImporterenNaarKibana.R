library(elastic)
library(rjson)

connect(es_host = "10.0.2.17", es_port = 9200,
        es_transport_schema = "http", es_user = "elastic", es_pwd = "zYk7LV0oe07xhvy4DsoT",
        errors = "simple")

index_delete(index = "poa5")
index_create(index = "poa5")

mappingPoa <- '{
"poa5": {
"properties": {
"course_over_ground": { "type": "keyword", "null_value": "NULL" },
"draught": {"type": "keyword", "null_value": "NULL"},
"fm_type": {"type": "text", "fields": { "keyword": {"type": "keyword"}}},
"latitude": {"type": "float" },
"length": {"type": "keyword", "null_value": "NULL"},
"longitude": {"type": "float"},
"location": {"type": "geo_point"},
"nav_status": {"type": "keyword", "null_value": "NULL"},
"received": {"type": "date"},
"speed_over_ground": {"type": "keyword", "null_value": "NULL"},
"true_heading": {"type": "keyword", "null_value": "NULL"},
"vessel_id": {"type": "long"},
"width": {"type": "keyword", "null_value": "NULL"}
}}}'

mapping_create(index = "poa5", type="poa5", body=mappingPoa)

setwd("C:/DATA/")
f = list.files(pattern = "*.*", full.names = TRUE)

for(j in f) {
  
  json_file <- j
  json_data <- fromJSON(file = json_file)
  json_data[is.na(json_data)] <- 0
  
  for (i in 1:length(json_data)) {
    location = ""
    if(json_data[[i]]["speed_over_ground"] == 'NULL'){ json_data[[i]]["speed_over_ground"] <- 'null' }
    if(json_data[[i]]["course_over_ground"] == 'NULL'){ json_data[[i]]["course_over_ground"] <- 'null' }
    if(json_data[[i]]["draught"] == 'NULL'){ json_data[[i]]["draught"] <- 'null' }
    if(json_data[[i]]["width"] == 'NULL'){ json_data[[i]]["width"] <- 'null' }
    if(json_data[[i]]["length"] == 'NULL'){ json_data[[i]]["length"] <- 'null' }
    if(json_data[[i]]["true_heading"] == 'NULL'){ json_data[[i]]["true_heading"] <- 'null' }
    if(json_data[[i]]["nav_status"] == 'NULL'){ json_data[[i]]["nav_status"] <- 'null' }
    longitude <- json_data[[i]][["longitude"]]
    latitude <- json_data[[i]][["latitude"]]
    location <- paste(latitude, ", ",longitude, sep="")
    json_data[[i]][["location"]] <- location
  }
  ## voor debug gebruik: capture.output(docs_bulk(json_data, index="poa5", es_ids = TRUE), file="output.txt", append = TRUE)
  docs_bulk(json_data, index="poa5", es_ids = TRUE)
}
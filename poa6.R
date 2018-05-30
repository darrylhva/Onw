
library(elastic)
library(rjson)

connect(es_host = "localhost", es_port = 9200,
        es_transport_schema = "http", errors = "simple")

index_delete(index = "poa6")
index_create(index = "poa6")
# mapping_get('poa4')

mappingPoa <- '{
"poa6": {
"properties": {
"course_over_ground": { "type": "float" },
"draught": {"type": "object"},
"fm_type": {"type": "text", "fields": { "keyword": {"type": "keyword", "ignore_above": 256}}},
"latitude": {"type": "float" },
"length": {"type": "long"},
"longitude": {"type": "float"},
"location": {"type": "geo_point"},
"nav_status": {"type": "text", "fields": {"keyword": {"type": "keyword","ignore_above": 256}}},
"received": {"type": "date"},
"speed_over_ground": {"type": "float"},
"true_heading": {"type": "object"},
"vessel_id": {"type": "long"},
"width": {"type": "long"}
}}}'

mapping_create(index = "poa6", type="poa6", body=mappingPoa)

setwd("F:/PortAmsterdamData/2016-03")
f = list.files(pattern = "*.*", full.names = TRUE)

for(j in f) {
  
  json_file <- j
  json_data <- fromJSON(file = json_file)
  json_data[is.na(json_data)] <- 0
  
  for (i in 1:length(json_data)) {
    location = ""
    json_data[[i]][["course_over_ground"]] <- NULL
    json_data[[i]][["speed_over_ground"]] <- NULL
    json_data[[i]][["draught"]] <- NULL  
    json_data[[i]][["true_heading"]] <- NULL
    json_data[[i]][["width"]] <- NULL
    json_data[[i]][["length"]] <- NULL
    json_data[[i]][["nav_status"]] <- NULL
    longitude <- json_data[[i]][["longitude"]]
    latitude <- json_data[[i]][["latitude"]]
    location <- paste(latitude, ", ",longitude, sep="")
    json_data[[i]][["location"]] <- location
  }
  capture.output(docs_bulk(json_data, index="poa6", es_ids = TRUE), file="output6.txt")
}

print("Klaar!!")
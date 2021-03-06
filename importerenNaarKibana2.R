library(elastic)
library(rjson)

connect(es_host = "localhost", es_port = 9200,
        es_transport_schema = "http", errors = "simple")

index_delete(index = "poa4")
index_create(index = "poa4")
# mapping_get('poa4')

mappingPoa <- '{
"poa4": {
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

mapping_create(index = "poa4", type="poa4", body=mappingPoa)

setwd("F:/PortAmsterdamData/2016-03/")
json_file <- "data.json"
json_data <- fromJSON(file = json_file)
json_data[is.na(json_data)] <- 0.0

for (i in 1:length(json_data)) {
  location = ""
  longitude <- json_data[[i]][["longitude"]]
  latitude <- json_data[[i]][["latitude"]]
  location <- paste(latitude, ", ",longitude, sep="")
  json_data[[i]][["location"]] <- location
}

docs_bulk(json_data, index="poa4", es_ids = TRUE)


library(elastic)
library(rjson)


connect(es_host = "localhost", es_port = 9200,
        es_transport_schema = "http", errors = "simple")

index_delete(index = "ligplaats1")
index_create(index = "ligplaats1")
# mapping_get('poa4')

mappingligplaats <- '{
"ligplaats1": {
      "properties": { 
        "OBJECTID":    { "type": "integer" }, 
"Postcode":     { "type": "text"  }, 
"Plaats":      { "type": "text" },  
"Diepgang_max": { "type": "float"},
"Longitude": {"type": "float"},
"Latitude": {"type": "float"},
"location": {"type": "geo_point"},
"Categorie": {"type": "text"},
"Scheepslengte_max":{ "type" :"integer"},
"Scheepsbreedte_max":{ "type" :"integer"},
"Ligplaats_Type": { "type": "text"}
}
}
}'

mapping_create(index = "ligplaats1", type="ligplaats1", body=mappingligplaats)

setwd("F:/PortAmsterdamData/2016-03")
f = list.files(pattern = "ligplaats.json", full.names = TRUE)

for(j in f) {
  
  json_file <- j
  json_data <- fromJSON(file = json_file)
  json_data[is.na(json_data)] <- 0
  
  for (i in 1:length(json_data)) {

    longitude <- json_data[[i]][["Longitude"]]
    latitude <- json_data[[i]][["Latitude"]]
    location <- paste(latitude, ", ",longitude, sep="")
    json_data[[i]][["location"]] <- location
  }
  capture.output(docs_bulk(json_data, index="ligplaats1", es_ids = TRUE), file="ligplaats1.txt")
}

print("Klaar!!")


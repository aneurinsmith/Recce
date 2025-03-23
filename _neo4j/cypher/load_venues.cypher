
//-------------------------//
// Create or update venues //
//-------------------------//

// Load amenity
LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/amenity.csv' AS row
WITH row
WHERE row.name IS NOT NULL

MERGE (v:Venue {venue_id: toString(row['@id'])})
SET v.venue_name = row.name,
    v.category = ['amenity', row.amenity],
    v.wikidata = toString(row.wikidata),
    v.location = point({
        latitude: toFloat(row['@lat']), 
        longitude: toFloat(row['@lon'])
    });

// Load tourism
LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/tourism.csv' AS row
WITH row
WHERE row.name IS NOT NULL

MERGE (v:Venue {venue_id: toString(row['@id'])})
SET v.venue_name = row.name,
    v.category = ['tourism', row.tourism],
    v.wikidata = toString(row.wikidata),
    v.location = point({
        latitude: toFloat(row['@lat']), 
        longitude: toFloat(row['@lon'])
    });

// Load natural
LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/natural.csv' AS row
WITH row
WHERE row.name IS NOT NULL

MERGE (v:Venue {venue_id: toString(row['@id'])})
SET v.venue_name = row.name,
    v.category = ['natural', row.natural],
    v.wikidata = toString(row.wikidata),
    v.location = point({
        latitude: toFloat(row['@lat']), 
        longitude: toFloat(row['@lon'])
    });

// Load leisure
LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/leisure.csv' AS row
WITH row
WHERE row.name IS NOT NULL

MERGE (v:Venue {venue_id: toString(row['@id'])})
SET v.venue_name = row.name,
    v.category = ['leisure', row.leisure],
    v.wikidata = toString(row.wikidata),
    v.location = point({
        latitude: toFloat(row['@lat']), 
        longitude: toFloat(row['@lon'])
    });

// Load historic
LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/historic.csv' AS row
WITH row
WHERE row.name IS NOT NULL

MERGE (v:Venue {venue_id: toString(row['@id'])})
SET v.venue_name = row.name,
    v.category = ['historic', row.historic],
    v.wikidata = toString(row.wikidata),
    v.location = point({
        latitude: toFloat(row['@lat']), 
        longitude: toFloat(row['@lon'])
    });


//----------------------//
// Link venues to stops //
//----------------------//

MATCH (s:Stop)
OPTIONAL MATCH (s)-[:CHILD_OF]->(p:Stop)

WITH DISTINCT CASE WHEN p IS NULL THEN s ELSE p END AS s

MATCH (v:Venue)
WITH v, s, point.distance(s.location, v.location) AS dist
WHERE dist < 1000

MERGE (s)-[:HAS {distWalked: dist}]->(v)

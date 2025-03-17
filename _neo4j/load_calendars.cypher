
//-------------------------//
// Delete unused calendars //
//-------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/calendar.txt' AS row
WITH collect(row.service_id) AS calendarIDs

// Delete all calendars
MATCH(c:Calendar)
WHERE NOT c.service_id IN calendarIDs

DETACH DELETE c;



//----------------------------//
// Create or update calendars //
//----------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/calendar.txt' AS row
WITH row

// Create or update loaded agencies
MERGE(c:Calendar {service_id: toString(row.service_id)})
SET c.mon = toBoolean(toInteger(row.monday)),
    c.tue = toBoolean(toInteger(row.tuesday)),
    c.wed = toBoolean(toInteger(row.wednesday)),
    c.thu = toBoolean(toInteger(row.thursday)),
    c.fri = toBoolean(toInteger(row.friday)),
    c.sat = toBoolean(toInteger(row.saturday)),
    c.sun = toBoolean(toInteger(row.sunday)),
    c.start_date = date(row.start_date),
    c.end_date = date(row.end_date);


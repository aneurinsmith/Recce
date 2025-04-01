
const Database = require('../config/neo4j')

class Venue {
    constructor() {
        this.db = new Database();
    }
    
    async find(params = {}) {

        if (!params.lat || !params.lng) {
            return {}
        } else {
            params.lat = parseFloat(params.lat);
            params.lng = parseFloat(params.lng);
        }

        if (!params.day || isNaN(parseInt(params.day))) {
            params.day = parseInt(new Date().getDay());
        } else {
            params.day = parseInt(params.day%7);
        }
        
        if (!params.time || isNaN(parseInt(params.time))) {
            const date = new Date();
            params.time = parseInt((date.getHours() * 3600) + (date.getMinutes() * 60) + date.getSeconds())
        } else {
            params.day += parseInt(Math.floor(params.time/86400)%7)
            params.time = parseInt(params.time%86400)
        }
        
        if (params.atco) {
            params.atcos = [params.atco]
        } else if (params.lat && params.lng) {
            const Stop = require('./stop')
            const stop = new Stop()

            params.atcos = await stop.find(params.lat, params.lng)
        } else {
            return {}
        }

        return this.db.execute(`

            WITH // Initial search params
                {time: $time, day: toInteger($day), location: point({latitude: toFloat($lat), longitude: toFloat($lng)})} AS curr

            WITH curr, $atcos AS atcos
            UNWIND atcos AS atco

            MATCH (fs:Stop {stop_id: toString(atco)})

            WITH curr, point.distance(fs.location, curr.location) AS dw1, fs
 
            MATCH // Match first stop timess
                (fs)<-[:CHILD_OF*0..1]-(:Stop)-[:TIME_TABLED]->(fst:StopTime)-[:PART_OF]->(t1:Trip)-[:BELONGS_TO]->(r1:Route),
                (t1)-[:RUNS_ON]->(c1:Calendar)
            WITH curr, dw1, fs, fst, r1 {route_name: r1.route_name, headsign: t1.headsign},
                (c1[toString(curr.day%7)] = true AND fst.departure_time > (curr.time + dw1)) AS today,
                (c1[toString((curr.day+1)%7)] = true AND fst.departure_time < (curr.time + dw1)) AS tommorrow
            WHERE today OR tommorrow
            WITH apoc.map.setKey(curr, 'day', case when today then curr.day else curr.day+1 end) AS curr, 
                dw1, fs, fst, r1, today, tommorrow

            ORDER BY curr.day, dw1, fst.departure_time
            WITH curr, dw1, fs, collect(fst) AS fsts, r1
            LIMIT 4
            UNWIND fsts AS fst
            ORDER BY curr.day, dw1, fst.departure_time
            WITH r1, collect(fst {curr, dw1, fs, fst, r1})[..20] AS fsts // max first stop times per route (ordered from closest day and time)
            ORDER BY fsts[0].dw1, fsts[0].fst.departure_time
            LIMIT 10
            WITH collect(r1) AS r1s, collect(fsts) AS fstss
            UNWIND fstss AS fsts
            UNWIND fsts AS fst
            ORDER BY fst.dw1, fst.curr.day, fst.fst.departure_time
            WITH fst.curr AS curr, fst.dw1 AS dw1, fst.fs AS fs, fst.fst AS fst, fst.r1 AS r1, r1s

            MATCH // Match paths from first stop times
                (fst)-[pr1:PRECEDES*2..20]->(st1:StopTime)<-[:TIME_TABLED]-(:Stop)-[:CHILD_OF*0..1]->(ts1:Stop)
            WHERE (NOT (ts1)-[:CHILD_OF]-(:Stop)) OR ((:Stop)-[:CHILD_OF]->(ts1))
            OPTIONAL MATCH // Match all nearby stops
                (ts1)-[n:NEARBY]-(ts2:Stop)
            ORDER BY fst, st1, n.distWalked
            WITH curr, dw1, fs, fst, pr1, st1, ts1, [null] + collect(ts2 {ts2, n})[..1] AS ts2s, r1, r1s // max transfer stops per first stop (ordered by distance)
            UNWIND ts2s AS ts2
            WITH curr, dw1, fs, fst, pr1, st1, coalesce(ts2.ts2, ts1) AS ts, coalesce(ts2.n.distWalked,null) AS dw2, r1, r1s

            OPTIONAL MATCH
                (ts)<-[:CHILD_OF*0..1]-(:Stop)-[tt:TIME_TABLED]->
                (st2:StopTime)-[pr2:PRECEDES*2..20]->(st3:StopTime)-[:PART_OF]->(t2:Trip)-[:BELONGS_TO]->(r2:Route),
                (t2)-[:RUNS_ON]-(c2:Calendar)
                WHERE size(pr1) + size(pr2) < 20
                AND st2.departure_time - st1.arrival_time < 1200 // should wait less then 20 minutes
                AND st2.departure_time - st1.arrival_time > 60 // should wait more then 1 minute
                AND st2.departure_time - st1.arrival_time > coalesce(dw2 * 1.2, 0) // should be walkable in time
                AND (
                        c2[toString(curr.day%7)] = true AND st2.departure_time > (st1.arrival_time % 86400) // should operate same day as c1...
                    OR c2[toString((curr.day+1)%7)] = true AND st2.departure_time < (st1.arrival_time % 86400) // ... or the day after
                    )
                AND NOT r2 {route_name: r2.route_name, headsign: t2.headsign} IN r1s // should be a different route

            WITH DISTINCT curr, dw1, fs, fst, pr1, st1, r1, tt, st2 {st2, dw2}, pr2, st3, r2 {route_name: r2.route_name, headsign: t2.headsign}
            WITH curr, dw1, fs, fst, pr1, st1, r1, [null] + collect(st2 {tt, dw2:st2.dw2, st2:st2.st2, pr2, st3, r2}) AS trs
            UNWIND trs AS tr
            WITH 
                curr, dw1, fs, fst, pr1, st1, r1, 
                tr.tt AS tt, tr.dw2 AS dw2, tr.st2 AS st2, tr.pr2 AS pr2, tr.st3 AS st3, tr.r2 AS r2, coalesce(tr.st3, st1) AS lst
            // WHERE st2 is not null // require transfer (debugging)

            MATCH (lst)<-[:TIME_TABLED]-(:Stop)-[:CHILD_OF*0..1]->(:Stop)-[h:HAS]->(v:Venue ${params.dest? '{venue_id: toString('+params.dest+')}' : ''})
            WHERE point.distance(v.location, curr.location) > 1000 + (50 * (size(pr1) + coalesce(size(pr2),0)))
            AND coalesce(dw1 + dw2 + h.distWalked, 0) < 1000
            AND v.category[0] = "tourism"

            WITH curr, dw1, fs, fst, r1, dw2, r2, h.distWalked AS dw3, v,
                pr1 + coalesce(tt, []) + coalesce(pr2, []) AS prs,
                coalesce(dw1,0) + 
                coalesce(reduce(total=0, r in pr1 | total + r.timeTraveled),0) +
                coalesce(st2.departure_time - st1.arrival_time,0) +
                coalesce(reduce(total=0, r in pr2 | total + r.timeTraveled),0) +
                coalesce(h.distWalked,0) 
                AS tt
            ORDER BY v, curr.day, tt, coalesce(dw2, 0)
            WHERE tt < 3600

            WITH v, collect(v {curr, dw1, fs, fst, r1, dw2, r2, dw3, prs, tt: tt-dw1-dw3})[0] AS cv
            UNWIND cv.prs AS pr

            MATCH ()-[pr]->(st:StopTime)
            MATCH (st)<-[:TIME_TABLED]-(sc:Stop)
            OPTIONAL MATCH (sc)-[:CHILD_OF]->(sp:Stop)

            ORDER BY v, cv.tt
            WITH v, cv, pr, st, coalesce(sp, sc) AS s

            RETURN cv.tt AS tt,
                {
                    stop_id: cv.fs.stop_id,
                    stop_name: cv.fs.stop_name,
                    location: [cv.fs.location.y, cv.fs.location.x],
                    departure_time: cv.fst.departure_time,
                    departure_day: cv.curr.day,
                    distance: cv.dw1
                } AS fs,
                {
                    venue_id: v.venue_id,
                    venue_name: v.venue_name,
                    location: [v.location.y, v.location.x],
                    category: v.category,
                    wikidata: v.wikidata,
                    distance: cv.dw3
                }AS v,
                collect(pr {
                    stop_id: s.stop_id,
                    stop_name: s.stop_name,
                    location: [s.location.y, s.location.x],
                    departure_time: st.departure_time,
                    relation: type(pr)
                }) AS j,
                [cv.r1] + coalesce(cv.r2, []) AS rs
            LIMIT 10

        `, params)
    }
}

module.exports = Venue;

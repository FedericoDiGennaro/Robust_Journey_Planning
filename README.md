# Robust Journey Planning

**Executive summary:** build a robust SBB journey planner for the Zürich area, and make a short video presentation of it.

## Team:
Bocini Ernesto  
Bourassa Maxime  
Brioschi Riccardo  
Culha Dafne  
Di Gennaro Federico  
Sternfeld Alexander  


----
## Content

* [HOW-TO](#HOW-TO)
* [Problem Motivation](#Problem-Motivation)
* [Problem Description](#Problem-Description)
* [Dataset Description](#Dataset-Description)

    - [Actual data](#Actual-data)
    - [Timetable data](#Timetable-data)
    - [Stations data](#Stations-data)
    - [Misc data](#Misc-data)


----
## HOW-TO

The correct pipeline to successfully run our code is the following:
* Run the `prepare_env.md` file to download and create all the tables needed (please notice that it takes a long time, so run it only if you do not have the tables!).  
* Run the `data_preparation.ipynb` file in order to pre-process the historical data (istdaten).  
* Finally, the route planning is on the `ER.ipynb` notebook where we import all the data, we implement our algorithm (CSA) to find paths and we find a way to get the probability to successfully complete the path and arrive on time.

## Problem Motivation

Imagine you are a regular user of the public transport system, and you are checking the operator's schedule to meet your friends for a class reunion.
The choices are:

1. You could leave in 10mins, and arrive with enough time to spare for gossips before the reunion starts.

2. You could leave now on a different route and arrive just in time for the reunion.

Undoubtedly, if this is the only information available, most of us will opt for option 1.

If we now tell you that option 1 carries a fifty percent chance of missing a connection and be late for the reunion. Whereas, option 2 is almost guaranteed to take you there on time. Would you still consider option 1?

Probably not. However, most public transport applications will insist on the first option. This is because they are programmed to plan routes that offer the shortest travel times, without considering the risk factors.

----
## Problem Description

In this final project we built our own _robust_ public transport route planner to improve on that. We use the SBB dataset (See next section: [Dataset Description](#dataset-description)).

Given a desired arrival time, our route planner will compute the fastest route between departure and arrival stops within a provided confidence tolerance expressed as interquartiles.
For instance, "what route from _A_ to _B_ is the fastest at least _Q%_ of the time if I want to arrive at _B_ before instant _T_". Note that *confidence* is a measure of a route being feasible within the travel time computed by the algorithm.

The output of the algorithm is a list of routes between _A_ and _B_ and their confidence levels. The routes must be sorted from latest (fastest) to earliest (longest) departure time at _A_, they must all arrive at _B_ before _T_ with a confidence level greater than or equal to _Q_. Ideally, it should be possible to visualize the routes on a map with straight lines connecting all the stops traversed by the route.

Solving this problem accurately can be difficult. For this reason we made a few **simplifying assumptions**:  
- We only consider journeys at reasonable hours of the day, and on a typical business day, and assuming a recent schedule.
- We allow short (total max 500m "As the Crows Flies") walking distances for transfers between two stops, and assume a walking speed of _50m/1min_ on a straight line, regardless of obstacles, human-built or natural, such as building, highways, rivers, or lakes.
- We only consider journeys that start and end on known station coordinates (train station, bus stops, etc.), never from a random location. However, walking from the departure stop to a nearby stop is allowed.
- We only consider departure and arrival stops in a 15km radius of Zürich's train station, `Zürich HB (8503000)`, (lat, lon) = `(47.378177, 8.540192)`.
- We only consider stops in the 15km radius that are reachable from Zürich HB. If needed stops may be reached via transfers through other stops outside the 15km area.
- There is no penalty for assuming that delays or travel times on the public transport network of **two different lines** are uncorrelated with one another, **however** try to capture dependenes
of stops on the same route, e.g. if a train is late at a stop, it is expected to be late at subsequent stops.
- Once a route is computed, a traveller is expected to follow the planned routes to the end, or until it fails (i.e. miss a connection).
  You **do not** need to address the case where travellers are able to defer their decisions and adapt their journey "en route", as more information becomes available. This would require us to consider all alternative routes (contingency plans) in the computation of the uncertainty levels, which is more difficult to implement.
- The planner will not need to mitigate the traveller's inconvenience if a plan fails. Two routes with identical travel times under the uncertainty tolerance are equivalent, even if the outcome of failing one route is much worse for the traveller than failing the other route, such as being stranded overnight on one route and not the other.
- All other things being equal, we will prefer routes with the minimum walking distance, and then minimum number of transfers.
- You do not need to optimize the computation time of your method, as long as the run-time is reasonable.
- When computing a path you may pick the timetable of the most recent week and assume that it is unchanged.

Upon request, and with clear instructions from you, we can help prepare the data in a form that is easier for you to process (within the limits of our ability, and time availability). In which case the data will be accessible to all.

----
## Dataset Description

For this project we will use the data published on the [Open Data Platform Mobility Switzerland](<https://opentransportdata.swiss>).

We will use the SBB data limited around the Zurich area, focusing only on stops within 15km of the Zurich main train station.

We will also provide you with a simulated realtime feed of Istdaten data.

#### Actual data

Students should already be familiar with the [istdaten](https://opentransportdata.swiss/de/dataset/istdaten).

The 2018 to 2022 data is available as a Hive table in partitioned ORC format on our HDFS system, under `/data/sbb/part_orc/istdaten`.

See assignments and exercises of earlier weeks for more information about this data, and methods to access it.

As a reminder, we provide the relevant column descriptions below.
The full description of the data is available in the opentransportdata.swiss data [istdaten cookbooks](https://opentransportdata.swiss/en/cookbook/actual-data/).

- `BETRIEBSTAG`: date of the trip
- `FAHRT_BEZEICHNER`: identifies the trip
- `BETREIBER_ABK`, `BETREIBER_NAME`: operator (name will contain the full name, e.g. Schweizerische Bundesbahnen for SBB)
- `PRODUCT_ID`: type of transport, e.g. train, bus
- `LINIEN_ID`: for trains, this is the train number
- `LINIEN_TEXT`,`VERKEHRSMITTEL_TEXT`: for trains, the service type (IC, IR, RE, etc.)
- `ZUSATZFAHRT_TF`: boolean, true if this is an additional trip (not part of the regular schedule)
- `FAELLT_AUS_TF`: boolean, true if this trip failed (cancelled or not completed)
- `HALTESTELLEN_NAME`: name of the stop
- `ANKUNFTSZEIT`: arrival time at the stop according to schedule
- `AN_PROGNOSE`: actual arrival time (see `AN_PROGNOSE_STATUS`)
- `AN_PROGNOSE_STATUS`: method used to measure `AN_PROGNOSE`, the time of arrival.
- `ABFAHRTSZEIT`: departure time at the stop according to schedule
- `AB_PROGNOSE`: actual departure time (see `AN_PROGNOSE_STATUS`)
- `AB_PROGNOSE_STATUS`: method used to measure  `AB_PROGNOSE`, the time of departure.
- `DURCHFAHRT_TF`: boolean, true if the transport does not stop there

Each line of the file represents a stop and contains arrival and departure times. When the stop is the start or end of a journey, the corresponding columns will be empty (`ANKUNFTSZEIT`/`ABFAHRTSZEIT`).
In some cases, the actual times were not measured so the `AN_PROGNOSE_STATUS`/`AB_PROGNOSE_STATUS` will be empty or set to `PROGNOSE` and `AN_PROGNOSE`/`AB_PROGNOSE` will be empty.

#### Timetable data

Timetable data are available from [timetable](https://opentransportdata.swiss/en/cookbook/gtfs/).

You will find there the timetables for the years [2018](https://opentransportdata.swiss/en/dataset/timetable-2018-gtfs), [2019](https://opentransportdata.swiss/en/dataset/timetable-2019-gtfs) and [2020](https://opentransportdata.swiss/en/dataset/timetable-2020-gtfs) and so on.
The timetables are updated weekly. It is ok to assume that the weekly changes are small, and a timetable for
a given week is thus the same for the full year - use the schedule of the most recent week for the day of the trip.

The full description of the GTFS format is available in the opentransportdata.swiss data [timetable cookbooks](https://opentransportdata.swiss/en/cookbook/gtfs/).

We provide a summary description of the files below:

* stops.txt(+):

    - `STOP_ID`: unique identifier (PK) of the stop
    - `STOP_NAME`: long name of the stop
    - `STOP_LAT`: stop latitude (WGS84)
    - `STOP_LON`: stop longitude
    - `LOCATION_TYPE`:
    - `PARENT_STATION`: if the stop is one of many collocated at a same location, such as platforms at a train station

* stop_times.txt(+):

    - `TRIP_ID`: identifier (FK) of the trip, unique for the day - e.g. _1.TA.1-100-j19-1.1.H_
    - `ARRIVAL_TIME`: scheduled (local) time of arrival at the stop (same as DEPARTURE_TIME if this is the start of the journey)
    - `DEPARTURE_TIME`: scheduled (local) time of departure at the stop 
    - `STOP_ID`: stop (station) identifier (FK), from stops.txt
    - `STOP_SEQUENCE`: sequence number of the stop on this trip id, starting at 1.
    - `PICKUP_TYPE`:
    - `DROP_OFF_TYPE`:

* trips.txt:

    - `ROUTE_ID`: identifier (FK) for the route. A route is a sequence of stops. It is time independent.
    - `SERVICE_ID`: identifier (FK) of a group of trips in the calendar, and for managing exceptions (e.g. holidays, etc).
    - `TRIP_ID`: is one instance (PK) of a vehicle journey on a given route - the same route can have many trips at regular intervals; a trip may skip some of the route stops.
    - `TRIP_HEADSIGN`: displayed to passengers, most of the time this is the (short) name of the last stop.
    - `TRIP_SHORT_NAME`: internal identifier for the trip_headsign (note TRIP_HEADSIGN and TRIP_SHORT_NAME are only unique for an agency)
    - `DIRECTION_ID`: if the route is bidirectional, this field indicates the direction of the trip on the route.
    
* calendar.txt:

    - `SERVICE_ID`: identifier (PK) of a group of trips sharing a same calendar and calendar exception pattern.
    - `MONDAY`..`SUNDAY`: 0 or 1 for each day of the week, indicating occurence of the service on that day.
    - `START_DATE`: start date when weekly service id pattern is valid
    - `END_DATE`: end date after which weekly service id pattern is no longer valid
    
* routes.txt:

    - `ROUTE_ID`: identifier for the route (PK)
    - `AGENCY_ID`: identifier of the operator (FK)
    - `ROUTE_SHORT_NAME`: the short name of the route, usually a line number
    - `ROUTE_LONG_NAME`: (empty)
    - `ROUTE_DESC`: _Bus_, _Zub_, _Tram_, etc.
    - `ROUTE_TYPE`:
    
**Note:** PK=Primary Key (unique), FK=Foreign Key (refers to a Primary Key in another table)

The other files are:

* _calendar-dates.txt_ contains exceptions to the weekly patterns expressed in _calendar.txt_.
* _agency.txt_ has the details of the operators
* _transfers.txt_ contains the transfer times between stops or platforms.

Figure 1. better illustrates the above concepts relating stops, routes, trips and stop times on a real example (route _11-3-A-j19-1_, direction _0_)


 ![journeys](figs/journeys.png)
 
 _Figure 1._ Relation between stops, routes, trips and stop times. The vertical axis represents the stops along the route in the direction of travel.
             The horizontal axis represents the time of day on a non-linear scale. Solid lines connecting the stops correspond to trips.
             A trip is one instances of a vehicle journey on the route. Trips on same route do not need
             to mark all the stops on the route, resulting in trips having different stop lists for the same route.


#### Stations data

For your convenience we also provide a consolidated liste of stop locations in ORC format under `/data/sbb/orc/allstops`. The schema of this table is the same as for the `stops.txt` format described earlier.

#### Misc data

Althought, not required for this final, you are of course free to use any other sources of data of your choice that might find helpful.

You may for instance download regions of openstreetmap [OSM](https://www.openstreetmap.org/#map=9/47.2839/8.1271&layers=TN),
which includes a public transport layer. If the planet OSM is too large for you,
you can find frequently updated exports of the [Swiss OSM region](https://planet.osm.ch/).

Others had some success using weather data to predict traffic delays.
If you want to give a try, web services such as [wunderground](https://www.wunderground.com/history/daily/ch/r%C3%BCmlang/LSZH/date/2022-1-1), can be a good
source of historical weather data.

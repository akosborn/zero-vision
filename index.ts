import * as dotenv from 'dotenv';
import {Client} from 'pg';

dotenv.config();

const run = async () => {
  const client = new Client({
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT as string),
    database: process.env.DB_NAME,
  });
  await client.connect();

  const highWaterMark = await findHighWaterMark(client);

  const whereClause = `first_occurrence_date>='${highWaterMark}'`;
  const encodedWhereClause = encodeURIComponent(whereClause);

  const orderByClause = `first_occurrence_date asc`;
  const encodedOrderByClause = encodeURIComponent(orderByClause);

  const url = `https://services1.arcgis.com/zdB7qR0BtYrg0Xpl/arcgis/rest/services/ODC_CRIME_TRAFFICACCIDENTS5YR_P/FeatureServer/325/query?where=${encodedWhereClause}&objectIds=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&collation=&orderByFields=${encodedOrderByClause}&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pjson&token=`;
  const response = await fetch(url);
  const data = await response.json();

  const features = data.features as Incident[];
  console.log(`Found ${features.length} incidents`);

  if (features.length === 0) {
    return;
  }

  const existingIncidents: { rows: { incident_id: string; }[] } = await client.query(`
    select incident_id
    from vision_zero.incidents_denver
    where incident_id in (${features.map((f) => `'${f.attributes.incident_id}'`).join(',')}) 
  `);
  console.log(`${existingIncidents.rowCount} of the incidents already exist`);

  const existingIncidentsByIncidentId = existingIncidents.rows.reduce<Record<number, true>>((acc, curr) => {
    acc[curr.incident_id] = true;
    return acc;
  }, {});

  const insertQuery = `
    INSERT INTO vision_zero.incidents_denver (
      object_id,
      incident_id,
      offense_id,
      offense_code,
      offense_code_extension,
      top_traffic_accident_offense,
      first_occurrence_date,
      last_occurrence_date,
      reported_date,
      incident_address,
      geo_x,
      geo_y,
      geo_lon,
      geo_lat,
      district_id,
      precinct_id,
      neighborhood_id,
      bicycle_ind,
      pedestrian_ind,
      harmful_event_seq_1,
      harmful_event_seq_2,
      harmful_event_seq_3,
      road_location,
      road_description,
      road_contour,
      road_condition,
      light_condition,
      tu1_vehicle_type,
      tu1_travel_direction,
      tu1_vehicle_movement,
      tu1_driver_action,
      tu1_driver_humancontribfactor,
      tu1_pedestrian_action,
      tu2_vehicle_type,
      tu2_travel_direction,
      tu2_vehicle_movement,
      tu2_driver_action,
      tu2_driver_humancontribfactor,
      tu2_pedestrian_action,
      seriously_injured,
      fatalities,
      fatality_mode_1,
      fatality_mode_2,
      seriously_injured_mode_1,
      seriously_injured_mode_2,
      point_x,
      point_y
    ) VALUES (
        $1,                                    -- object_id (bigint)
        $2,                                    -- incident_id (varchar)
        $3,                                    -- offense_id (varchar)
        $4,                                    -- offense_code (varchar)
        $5,                                    -- offense_code_extension (char)
        $6,                                    -- top_traffic_accident_offense (varchar)
        $7,                                    -- first_occurrence_date (timestamp)
        $8,                                    -- last_occurrence_date (timestamp)
        $9,                                    -- reported_date (timestamp)
        $10,                                   -- incident_address (varchar)
        $11,                                   -- geo_x (integer)
        $12,                                   -- geo_y (integer)
        $13,                                   -- geo_lon (numeric)
        $14,                                   -- geo_lat (numeric)
        $15,                                   -- district_id (varchar)
        $16,                                   -- precinct_id (varchar)
        $17,                                   -- neighborhood_id (varchar)
        $18,                                   -- bicycle_ind (smallint)
        $19,                                   -- pedestrian_ind (smallint)
        $20,                                   -- harmful_event_seq_1 (varchar)
        $21,                                   -- harmful_event_seq_2 (varchar)
        $22,                                   -- harmful_event_seq_3 (varchar)
        $23,                                   -- road_location (varchar)
        $24,                                   -- road_description (varchar)
        $25,                                   -- road_contour (varchar)
        $26,                                   -- road_condition (varchar)
        $27,                                   -- light_condition (varchar)
        $28,                                   -- tu1_vehicle_type (varchar)
        $29,                                   -- tu1_travel_direction (varchar)
        $30,                                   -- tu1_vehicle_movement (varchar)
        $31,                                   -- tu1_driver_action (varchar)
        $32,                                   -- tu1_driver_humancontribfactor (varchar)
        $33,                                   -- tu1_pedestrian_action (varchar)
        $34,                                   -- tu2_vehicle_type (varchar)
        $35,                                   -- tu2_travel_direction (varchar)
        $36,                                   -- tu2_vehicle_movement (varchar)
        $37,                                   -- tu2_driver_action (varchar)
        $38,                                   -- tu2_driver_humancontribfactor (varchar)
        $39,                                   -- tu2_pedestrian_action (varchar)
        $40,                                   -- seriously_injured (smallint)
        $41,                                   -- fatalities (smallint)
        $42,                                   -- fatality_mode_1 (varchar)
        $43,                                   -- fatality_mode_2 (varchar)
        $44,                                   -- seriously_injured_mode_1 (varchar)
        $45,                                   -- seriously_injured_mode_2 (varchar)
        $46,                                   -- point_x (numeric)
        $47                                    -- point_y (numeric)
    );
  `;

  await Promise.all(features.map(async ({ attributes }) => {
    const firstOccurrenceDate = new Date(attributes.first_occurrence_date).toISOString();
    const lastOccurrenceDate = new Date(attributes.last_occurrence_date).toISOString();
    const reportedDate = new Date(attributes.reported_date).toISOString();

    if (existingIncidentsByIncidentId[attributes.incident_id]) {
      const query = `
        UPDATE vision_zero.incidents_denver
        SET
          offense_id = '${attributes.offense_id}',
          offense_code = '${attributes.offense_code}',
          offense_code_extension = '${attributes.offense_code_extension}',
          top_traffic_accident_offense = '${attributes.top_traffic_accident_offense}',
          first_occurrence_date = '${firstOccurrenceDate}',
          last_occurrence_date = '${lastOccurrenceDate}',
          reported_date = '${reportedDate}',
          incident_address = '${attributes.incident_address.replaceAll(`'`, `''`)}',
          geo_x = ${attributes.geo_x},
          geo_y = ${attributes.geo_y},
          geo_lon = ${attributes.geo_lon},
          geo_lat = ${attributes.geo_lat},
          district_id = '${attributes.district_id}',
          precinct_id = '${attributes.precinct_id ?? ''}',
          neighborhood_id = '${attributes.neighborhood_id ?? ''}',
          bicycle_ind = ${attributes.bicycle_ind},
          pedestrian_ind = ${attributes.pedestrian_ind},
          harmful_event_seq_1 = '${attributes.HARMFUL_EVENT_SEQ_1}',
          harmful_event_seq_2 = '${attributes.HARMFUL_EVENT_SEQ_2}',
          harmful_event_seq_3 = '${attributes.HARMFUL_EVENT_SEQ_3}',
          road_location = '${attributes.road_location?.replaceAll(`'`, `''`)}',
          road_description = '${attributes.ROAD_DESCRIPTION}',
          road_contour = '${attributes.ROAD_CONTOUR}',
          road_condition = '${attributes.ROAD_CONDITION}',
          light_condition = '${attributes.LIGHT_CONDITION}',
          tu1_vehicle_type = '${attributes.TU1_VEHICLE_TYPE}',
          tu1_travel_direction = '${attributes.TU1_TRAVEL_DIRECTION}',
          tu1_vehicle_movement = '${attributes.TU1_VEHICLE_MOVEMENT}',
          tu1_driver_action = '${attributes.TU1_DRIVER_ACTION}',
          tu1_driver_humancontribfactor = '${attributes.TU1_DRIVER_HUMANCONTRIBFACTOR}',
          tu1_pedestrian_action = '${attributes.TU1_PEDESTRIAN_ACTION}',
          tu2_vehicle_type = '${attributes.TU2_VEHICLE_TYPE}',
          tu2_travel_direction = '${attributes.TU2_TRAVEL_DIRECTION}',
          tu2_vehicle_movement = '${attributes.TU2_VEHICLE_MOVEMENT}',
          tu2_driver_action = '${attributes.TU2_DRIVER_ACTION}',
          tu2_driver_humancontribfactor = '${attributes.TU2_DRIVER_HUMANCONTRIBFACTOR}',
          tu2_pedestrian_action = '${attributes.TU2_PEDESTRIAN_ACTION}',
          seriously_injured = ${attributes.SERIOUSLY_INJURED},
          fatalities = ${attributes.FATALITIES},
          fatality_mode_1 = '${attributes.FATALITY_MODE_1}',
          fatality_mode_2 = '${attributes.FATALITY_MODE_2}',
          seriously_injured_mode_1 = '${attributes.SERIOUSLY_INJURED_MODE_1}',
          seriously_injured_mode_2 = '${attributes.SERIOUSLY_INJURED_MODE_2}',
          point_x = ${attributes.point_x || null},
          point_y = ${attributes.point_y || null}
        WHERE incident_id = '${attributes.incident_id}'
        ;
      `
      return client.query(query).catch((err) => {
        console.log('Update error', query);
        console.log('Update error', attributes);
        console.log('Update error', err);
      });
    }

    return client.query(insertQuery, [
      attributes.object_id,
      attributes.incident_id,
      attributes.offense_id,
      attributes.offense_code,
      attributes.offense_code_extension,
      attributes.top_traffic_accident_offense,
      firstOccurrenceDate,
      lastOccurrenceDate,
      reportedDate,
      attributes.incident_address,
      attributes.geo_x,
      attributes.geo_y,
      attributes.geo_lon,
      attributes.geo_lat,
      attributes.district_id,
      attributes.precinct_id,
      attributes.neighborhood_id,
      attributes.bicycle_ind,
      attributes.pedestrian_ind,
      attributes.HARMFUL_EVENT_SEQ_1,
      attributes.HARMFUL_EVENT_SEQ_2,
      attributes.HARMFUL_EVENT_SEQ_3,
      attributes.road_location,
      attributes.ROAD_DESCRIPTION,
      attributes.ROAD_CONTOUR,
      attributes.ROAD_CONDITION,
      attributes.LIGHT_CONDITION,
      attributes.TU1_VEHICLE_TYPE,
      attributes.TU1_TRAVEL_DIRECTION,
      attributes.TU1_VEHICLE_MOVEMENT,
      attributes.TU1_DRIVER_ACTION,
      attributes.TU1_DRIVER_HUMANCONTRIBFACTOR,
      attributes.TU1_PEDESTRIAN_ACTION,
      attributes.TU2_VEHICLE_TYPE,
      attributes.TU2_TRAVEL_DIRECTION,
      attributes.TU2_VEHICLE_MOVEMENT,
      attributes.TU2_DRIVER_ACTION,
      attributes.TU2_DRIVER_HUMANCONTRIBFACTOR,
      attributes.TU2_PEDESTRIAN_ACTION,
      attributes.SERIOUSLY_INJURED,
      attributes.FATALITIES,
      attributes.FATALITY_MODE_1,
      attributes.FATALITY_MODE_2,
      attributes.SERIOUSLY_INJURED_MODE_1,
      attributes.SERIOUSLY_INJURED_MODE_2,
      attributes.POINT_X,
      attributes.POINT_Y,
    ]).catch((err) => {
      console.log('Insert error', err);
    });
  })).catch((err) => {
    console.log(err);
  });

  await client.end();
};

const findHighWaterMark = async (client: Client) => {
  if (process.env.HIGH_WATER_MARK) {
    return process.env.HIGH_WATER_MARK;
  }

  const results = await client.query<{ high_water_mark: Date }>(`
    select (max(first_occurrence_date) - '1 day'::interval)::date as high_water_mark
    from vision_zero.incidents_denver
  `);
  return results.rows[0].high_water_mark.toISOString().slice(0, 10);
};

run();

type Incident = {
  attributes: {
    object_id: number;
    incident_id: string;
    offense_id: string;
    offense_code: string;
    offense_code_extension: string;
    top_traffic_accident_offense: string;
    first_occurrence_date: number;
    last_occurrence_date: number;
    reported_date: number;
    incident_address: string;
    geo_x: number;
    geo_y: number;
    geo_lon: number;
    geo_lat: number;
    district_id: string;
    precinct_id: string;
    neighborhood_id: string;
    bicycle_ind: number;
    pedestrian_ind: number;
    HARMFUL_EVENT_SEQ_1: string;
    HARMFUL_EVENT_SEQ_2: string;
    HARMFUL_EVENT_SEQ_3: string;
    road_location: string;
    ROAD_DESCRIPTION: string;
    ROAD_CONTOUR: string;
    ROAD_CONDITION: string;
    LIGHT_CONDITION: string;
    TU1_VEHICLE_TYPE: string;
    TU1_TRAVEL_DIRECTION: string;
    TU1_VEHICLE_MOVEMENT: string;
    TU1_DRIVER_ACTION: string;
    TU1_DRIVER_HUMANCONTRIBFACTOR: string;
    TU1_PEDESTRIAN_ACTION: string;
    TU2_VEHICLE_TYPE: string;
    TU2_TRAVEL_DIRECTION: string;
    TU2_VEHICLE_MOVEMENT: string;
    TU2_DRIVER_ACTION: string;
    TU2_DRIVER_HUMANCONTRIBFACTOR: string;
    TU2_PEDESTRIAN_ACTION: string;
    SERIOUSLY_INJURED: number;
    FATALITIES: number;
    FATALITY_MODE_1: string;
    FATALITY_MODE_2: string;
    SERIOUSLY_INJURED_MODE_1: string;
    SERIOUSLY_INJURED_MODE_2: string;
    POINT_X: number | null;
    POINT_Y: number | null;
  };
};

DROP TRIGGER IF EXISTS gtfs.shape_geom_trigger ON gtfs.shapes;

DROP TRIGGER IF EXISTS gtfs.stop_times_dist_row_trigger ON gtfs.stop_times;

DROP TRIGGER IF EXISTS gtfs.stop_times_dist_stmt_trigger ON gtfs.stop_times;

DROP TRIGGER IF EXISTS gtfs.stop_geom_trigger ON gtfs.stops;

DROP TRIGGER IF EXISTS gtfs.add_to_calendars ON gtfs.calendar_dates;

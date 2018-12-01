CREATE TRIGGER gtfs.shape_geom_trigger AFTER INSERT ON gtfs.shapes
    FOR EACH STATEMENT EXECUTE PROCEDURE gtfs.shape_update();

CREATE TRIGGER gtfs.stop_times_dist_row_trigger BEFORE INSERT ON gtfs.stop_times
  FOR EACH ROW
  WHEN (NEW.shape_dist_traveled IS NULL)
  EXECUTE PROCEDURE gtfs.dist_insert();

CREATE TRIGGER gtfs.stop_times_dist_stmt_trigger AFTER INSERT ON gtfs.stop_times
  FOR EACH STATEMENT EXECUTE PROCEDURE gtfs.dist_update();

CREATE TRIGGER gtfs.stop_geom_trigger BEFORE INSERT OR UPDATE ON gtfs.stops
    FOR EACH ROW EXECUTE PROCEDURE gtfs.stop_geom_update();

CREATE TRIGGER add_to_calendars
  BEFORE INSERT OR UPDATE
  ON gtfs.calendar_dates
  FOR EACH ROW
  EXECUTE PROCEDURE gtfs.calendar_insert();

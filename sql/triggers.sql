CREATE TRIGGER gtfs_shape_geom_trigger AFTER INSERT ON gtfs_shapes
    FOR EACH STATEMENT EXECUTE PROCEDURE gtfs_shape_update();

CREATE TRIGGER gtfs_stop_times_dist_trigger AFTER INSERT ON gtfs_stop_times
    FOR EACH STATEMENT EXECUTE PROCEDURE gtfs_dist_update();

CREATE TRIGGER gtfs_stop_geom_trigger BEFORE INSERT OR UPDATE ON gtfs_stops
    FOR EACH ROW EXECUTE PROCEDURE gtfs_stop_geom_update();

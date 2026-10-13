ALTER TABLE dm.session_overview_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.session_overview_stage_rep;
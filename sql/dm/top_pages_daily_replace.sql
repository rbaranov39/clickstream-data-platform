ALTER TABLE dm.top_pages_daily_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.top_pages_daily_stage_rep;

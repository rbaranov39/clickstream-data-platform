ALTER TABLE dm.dq_errors_daily_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dm.dq_errors_daily_stage_rep;
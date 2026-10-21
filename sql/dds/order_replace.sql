ALTER TABLE dds.order_rep ON CLUSTER clickstream_cluster
REPLACE PARTITION '{{ params.day }}' FROM dds.order_stage_rep;
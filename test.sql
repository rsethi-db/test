-- Databricks notebook source
SELECT a.sfdcAccountName,
  a.date,
       a.yyyymm as month,
       a.workspaceName,
       case when a.sku like '%DLT%' then 'DLT'  
       			when a.sku like '%SERVERLESS%' then 'Serverless SQL'
            else a.workloadType end workloadType ,
       a.sku,
       a.workspaceId,
       a.shardName,
       SUM(dbus) AS DBUs,
       sum(dbus * revSharePrice) as dollars,
       MAX(a.date)
FROM prod.workloads_cluster_agg a
LEFT JOIN prod.effective_rates b
Using(date, workspaceId, sku)
WHERE (a.sfdcAccountName = 'Nationwide')
  AND (a.date >= '2023-01-01')
GROUP BY a.sfdcAccountName,
         a.yyyymm,
       case when a.sku like '%DLT%' then 'DLT'  
       			when a.sku like '%SERVERLESS%' then 'Serverless SQL'
            else a.workloadType end,
         a.workspaceName,
         a.sku,
         a.shardName,
         a.workspaceId,
  1,
  2,
  3,
  4,
  5,
  6,
  7

-- COMMAND ----------



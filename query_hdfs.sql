SELECT COUNT(*) as total_archived_orders
FROM parquet.`hdfs://namenode:9000/archives/ecommerce/archive_1766991842`;

SELECT 
    product.category,
    COUNT(*) as order_count,
    SUM(`order`.total_amount) as total_revenue
FROM parquet.`hdfs://namenode:9000/archives/ecommerce/archive_1766991842`
GROUP BY product.category
ORDER BY total_revenue DESC
LIMIT 5;
WITH latest_satellite_data AS (
    SELECT sale_id, sales, quantity, discount, profit
    FROM (
        SELECT sale_id, sales, quantity, discount, profit, effective_from,
               ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY effective_from DESC) as rn
        FROM sat_sales
    ) ss
    WHERE ss.rn = 1
),
latest_shipments AS (
    SELECT shipment_id, shipment_mode
    FROM (
        SELECT shipment_id, shipment_mode, effective_from,
               ROW_NUMBER() OVER (PARTITION BY shipment_id ORDER BY effective_from DESC) as rn
        FROM sat_shipments
    ) sship
    WHERE sship.rn = 1
),
latest_clients AS (
    SELECT client_id, client_segment
    FROM (
        SELECT client_id, client_segment, effective_from,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY effective_from DESC) as rn
        FROM sat_clients
    ) sc
    WHERE sc.rn = 1
),
latest_stores AS (
    SELECT store_id, country, city, state, postal_code, region
    FROM (
        SELECT store_id, country, city, state, postal_code, region, effective_from,
               ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY effective_from DESC) as rn
        FROM sat_store
    ) sst
    WHERE sst.rn = 1
),
latest_products AS (
    SELECT product_id, category, subcategory
    FROM (
        SELECT product_id, category, subcategory, effective_from,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY effective_from DESC) as rn
        FROM sat_products
    ) sp
    WHERE sp.rn = 1
)
SELECT 
    hs.sale_id,
    ss.sales,
    ss.quantity,
    ss.discount,
    ss.profit,
    sc.client_segment,
    sst.country,
    sst.city,
    sst.state,
    sst.postal_code,
    sst.region,
    sship.shipment_mode,
    sp.category,
    sp.subcategory,
    hs.created_at as sale_date
FROM hub_sales hs
INNER JOIN latest_satellite_data ss ON hs.sale_id = ss.sale_id
INNER JOIN link_sales_clients lsc ON hs.sale_id = lsc.sale_id
INNER JOIN latest_clients sc ON lsc.client_id = sc.client_id
INNER JOIN link_sales_stores lst ON hs.sale_id = lst.sale_id
INNER JOIN latest_stores sst ON lst.store_id = sst.store_id
INNER JOIN link_sales_shipments lss ON hs.sale_id = lss.sale_id
INNER JOIN latest_shipments sship ON lss.shipment_id = sship.shipment_id
INNER JOIN link_sales_products lsp ON hs.sale_id = lsp.sale_id
INNER JOIN latest_products sp ON lsp.product_id = sp.product_id
ORDER BY hs.created_at DESC;

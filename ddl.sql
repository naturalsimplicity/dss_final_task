-- Создание hub-таблиц
CREATE TABLE hub_shipments (
    shipment_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shipment_id)
) DISTRIBUTED BY (shipment_id);

CREATE TABLE hub_clients (
    client_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id)
) DISTRIBUTED BY (client_id);

CREATE TABLE hub_stores (
    store_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id)
) DISTRIBUTED BY (store_id);

CREATE TABLE hub_products (
    product_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
) DISTRIBUTED BY (product_id);

CREATE TABLE hub_sales (
    sale_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id)
) DISTRIBUTED BY (sale_id);

-- Создание link-таблиц
CREATE TABLE link_sales_shipments (
    sale_id VARCHAR(255) NOT NULL,
    shipment_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, shipment_id),
    FOREIGN KEY (sale_id) REFERENCES hub_sales(sale_id),
    FOREIGN KEY (shipment_id) REFERENCES hub_shipments(shipment_id)
) DISTRIBUTED BY (sale_id);

CREATE TABLE link_sales_clients (
    sale_id VARCHAR(255) NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, client_id),
    FOREIGN KEY (sale_id) REFERENCES hub_sales(sale_id),
    FOREIGN KEY (client_id) REFERENCES hub_clients(client_id)
) DISTRIBUTED BY (sale_id);

CREATE TABLE link_sales_stores (
    sale_id VARCHAR(255) NOT NULL,
    store_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, store_id),
    FOREIGN KEY (sale_id) REFERENCES hub_sales(sale_id),
    FOREIGN KEY (store_id) REFERENCES hub_stores(store_id)
) DISTRIBUTED BY (sale_id);

CREATE TABLE link_sales_products (
    sale_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, product_id),
    FOREIGN KEY (sale_id) REFERENCES hub_sales(sale_id),
    FOREIGN KEY (product_id) REFERENCES hub_products(product_id)
) DISTRIBUTED BY (sale_id);

-- Создание sat-таблиц
CREATE TABLE sat_shipments (
    shipment_id VARCHAR(255) NOT NULL,
    shipment_mode VARCHAR(100),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shipment_id, effective_from),
    FOREIGN KEY (shipment_id) REFERENCES hub_shipments(shipment_id)
) DISTRIBUTED BY (shipment_id);

CREATE TABLE sat_clients (
    client_id VARCHAR(255) NOT NULL,
    client_segment VARCHAR(100),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id, effective_from),
    FOREIGN KEY (client_id) REFERENCES hub_clients(client_id)
) DISTRIBUTED BY (client_id);

CREATE TABLE sat_store (
    store_id VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(100),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, effective_from),
    FOREIGN KEY (store_id) REFERENCES hub_stores(store_id)
) DISTRIBUTED BY (store_id);

CREATE TABLE sat_products (
    product_id VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, effective_from),
    FOREIGN KEY (product_id) REFERENCES hub_products(product_id)
) DISTRIBUTED BY (product_id);

CREATE TABLE sat_sales (
    sale_id VARCHAR(255) NOT NULL,
    sales DECIMAL(15,2),
    quantity DECIMAL(15,2),
    discount DECIMAL(15,2),
    profit DECIMAL(15,2),
    effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, effective_from),
    FOREIGN KEY (sale_id) REFERENCES hub_sales(sale_id)
) DISTRIBUTED BY (sale_id);

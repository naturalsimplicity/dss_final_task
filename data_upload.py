import pandas as pd
import hashlib
from sqlalchemy import create_engine
from datetime import datetime


def generate_md5_hash(value):
    return hashlib.md5(str(value).encode()).hexdigest()


def load_data_to_datavault(csv_file_path, db_connection_string):
    df = pd.read_csv(csv_file_path)
    df = df.reset_index().rename(columns={'index': 'row_number'})
    engine = create_engine(db_connection_string)
    current_timestamp = datetime.now()
    
    print("Подготовка HUB-таблиц...")
    
    # HUB Shipments
    hub_shipments = pd.DataFrame({
        'shipment_id': df['Ship Mode'].apply(generate_md5_hash),
        'created_at': current_timestamp
    }).drop_duplicates('shipment_id')
    
    # HUB Clients (используем хэш от порядкового номера)
    hub_clients = pd.DataFrame({
        'client_id': df['row_number'].apply(lambda x: generate_md5_hash(f"client_{x}")),
        'created_at': current_timestamp
    }).drop_duplicates('client_id')
    
    # HUB Stores
    hub_stores = pd.DataFrame({
        'store_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Country']}_{row['City']}_{row['State']}_{row['Postal Code']}_{row['Region']}"
        ), axis=1),
        'created_at': current_timestamp
    }).drop_duplicates('store_id')
    
    # HUB Products
    hub_products = pd.DataFrame({
        'product_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Category']}_{row['Sub-Category']}"
        ), axis=1),
        'created_at': current_timestamp
    }).drop_duplicates('product_id')
    
    # HUB Sales (используем хэш от комбинации полей для уникальности)
    hub_sales = pd.DataFrame({
        'sale_id': df.apply(lambda row: generate_md5_hash(
            f"{row['row_number']}_{row['Sales']}_{row['Quantity']}_{row['Discount']}_{row['Profit']}"
        ), axis=1),
        'created_at': current_timestamp
    }).drop_duplicates('sale_id')
    
    # Загрузка HUB-таблиц
    hub_shipments.to_sql('hub_shipments', engine, if_exists='append', index=False)
    hub_clients.to_sql('hub_clients', engine, if_exists='append', index=False)
    hub_stores.to_sql('hub_stores', engine, if_exists='append', index=False)
    hub_products.to_sql('hub_products', engine, if_exists='append', index=False)
    hub_sales.to_sql('hub_sales', engine, if_exists='append', index=False)
    
    print("HUB-таблицы загружены")
    
    # 2. Подготовка и загрузка LINK-таблиц
    print("Подготовка LINK-таблиц...")
    
    # Создаем DataFrame с ключами для связей
    links_df = pd.DataFrame({
        'sale_id': hub_sales['sale_id'],
        'shipment_id': df['Ship Mode'].apply(generate_md5_hash),
        'client_id': df['row_number'].apply(lambda x: generate_md5_hash(f"client_{x}")),
        'store_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Country']}_{row['City']}_{row['State']}_{row['Postal Code']}_{row['Region']}"
        ), axis=1),
        'product_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Category']}_{row['Sub-Category']}"
        ), axis=1),
        'created_at': current_timestamp
    })
    
    # LINK Sales-Shipments
    link_sales_shipments = links_df[['sale_id', 'shipment_id', 'created_at']].drop_duplicates()
    link_sales_shipments.to_sql('link_sales_shipments', engine, if_exists='append', index=False)
    
    # LINK Sales-Clients
    link_sales_clients = links_df[['sale_id', 'client_id', 'created_at']].drop_duplicates()
    link_sales_clients.to_sql('link_sales_clients', engine, if_exists='append', index=False)
    
    # LINK Sales-Stores
    link_sales_stores = links_df[['sale_id', 'store_id', 'created_at']].drop_duplicates()
    link_sales_stores.to_sql('link_sales_stores', engine, if_exists='append', index=False)
    
    # LINK Sales-Products
    link_sales_products = links_df[['sale_id', 'product_id', 'created_at']].drop_duplicates()
    link_sales_products.to_sql('link_sales_products', engine, if_exists='append', index=False)
    
    print("LINK-таблицы загружены")
    
    # 3. Подготовка и загрузка SAT-таблиц
    print("Подготовка SAT-таблиц...")
    
    # SAT Shipments
    sat_shipments = pd.DataFrame({
        'shipment_id': df['Ship Mode'].apply(generate_md5_hash),
        'shipment_mode': df['Ship Mode'],
        'effective_from': current_timestamp
    }).drop_duplicates('shipment_id')
    sat_shipments.to_sql('sat_shipments', engine, if_exists='append', index=False)
    
    # SAT Clients
    sat_clients = pd.DataFrame({
        'client_id': df['row_number'].apply(lambda x: generate_md5_hash(f"client_{x}")),
        'client_segment': df['Segment'],
        'effective_from': current_timestamp
    }).drop_duplicates('client_id')
    sat_clients.to_sql('sat_clients', engine, if_exists='append', index=False)
    
    # SAT Store
    sat_store = pd.DataFrame({
        'store_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Country']}_{row['City']}_{row['State']}_{row['Postal Code']}_{row['Region']}"
        ), axis=1),
        'country': df['Country'],
        'city': df['City'],
        'state': df['State'],
        'postal_code': df['Postal Code'],
        'region': df['Region'],
        'effective_from': current_timestamp
    }).drop_duplicates('store_id')
    sat_store.to_sql('sat_store', engine, if_exists='append', index=False)
    
    # SAT Products
    sat_products = pd.DataFrame({
        'product_id': df.apply(lambda row: generate_md5_hash(
            f"{row['Category']}_{row['Sub-Category']}"
        ), axis=1),
        'category': df['Category'],
        'subcategory': df['Sub-Category'],
        'effective_from': current_timestamp
    }).drop_duplicates('product_id')
    sat_products.to_sql('sat_products', engine, if_exists='append', index=False)
    
    # SAT Sales
    sat_sales = pd.DataFrame({
        'sale_id': hub_sales['sale_id'],
        'sales': df['Sales'],
        'quantity': df['Quantity'],
        'discount': df['Discount'],
        'profit': df['Profit'],
        'effective_from': current_timestamp
    }).drop_duplicates('sale_id')
    sat_sales.to_sql('sat_sales', engine, if_exists='append', index=False)
    
    print("SAT-таблицы загружены")
    print("Все данные успешно загружены в Data Vault модель!")

if __name__ == "__main__":
    CSV_FILE_PATH = "SampleSuperstore.csv"
    DB_CONNECTION_STRING = "postgresql+psycopg2://mgvasheka:jatDav-pamgy3-haksyr@rc1d-jem8l3mj641ffo29.mdb.yandexcloud.net:6432/hse"
    
    load_data_to_datavault(CSV_FILE_PATH, DB_CONNECTION_STRING)

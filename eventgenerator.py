import pandas as pd
import time
from sqlalchemy import create_engine, text

# Connexion avec le port 5433 et le mot de passe adminSS
engine = create_engine('postgresql://admin:adminSS@localhost:5433/source_db')

def simulate_real_time_sales():
    print("--- Nettoyage et Chargement des données de référence ---")
    
    with engine.connect() as conn:
        # On vide les tables dans l'ordre inverse des dépendances (Cascade)
        conn.execute(text("TRUNCATE TABLE order_items, orders, customers, products, category_translation RESTART IDENTITY CASCADE;"))
        conn.commit()

    # On utilise 'append' maintenant que les tables sont vides
    pd.read_csv('datasets/olist_customers_dataset.csv').to_sql('customers', engine, if_exists='append', index=False)
    pd.read_csv('datasets/olist_products_dataset.csv').to_sql('products', engine, if_exists='append', index=False)
    pd.read_csv('datasets/product_category_name_translation.csv').to_sql('category_translation', engine, if_exists='append', index=False)
    
    print("--- Début de la simulation temps réel (Streaming) ---")
    orders = pd.read_csv('datasets/olist_orders_dataset.csv')
    items = pd.read_csv('datasets/olist_order_items_dataset.csv')

    for i in range(len(orders)):
        try:
            order_row = orders.iloc[[i]]
            order_row.to_sql('orders', engine, if_exists='append', index=False)

            order_id = order_row['order_id'].values[0]
            items_to_insert = items[items['order_id'] == order_id]
            items_to_insert.to_sql('order_items', engine, if_exists='append', index=False)

            print(f"[{time.strftime('%H:%M:%S')}] Commande insérée : {order_id}")
            time.sleep(1) 
            
        except Exception as e:
            print(f"Erreur sur la commande {i}: {e}")
            continue

if __name__ == "__main__":
    simulate_real_time_sales()
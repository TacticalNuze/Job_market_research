import psycopg2

try:
    conn = psycopg2.connect("user=root password=123456 host=postgres dbname=offers port=5432")
    print("✅ Connexion réussie.")
    conn.close()
except Exception as e:
    print("❌ Erreur :", repr(e))

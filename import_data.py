import argparse
import os
import sys
import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

def parse_arguments():
    parser = argparse.ArgumentParser(description="Impor dataset RetailRocket (events.csv) ke database Neo4J.")
    parser.add_argument("-u", "--user", default="neo4j", help="Username Neo4j (default: neo4j)")
    parser.add_argument("-p", "--password", default="password", help="Password Neo4J kamu") # Ganti password ini
    parser.add_argument("-b", "--bolt", default="bolt://localhost:7687", help="Neo4J URI / Bolt")
    parser.add_argument("-s", "--source", default="data", help="Direktori/folder sumber data")
    return parser.parse_args()

def create_constraints(session):
    # Buat constraint indeks biar proses query nanti cepet
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (v:Visitor) REQUIRE v.visitorId IS UNIQUE;")
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Item) REQUIRE i.itemId IS UNIQUE;")

def import_batch(tx, batch):
    cypher_query = """
    UNWIND $batch AS row
    MERGE (v:Visitor {visitorId: row.visitorid})
    MERGE (i:Item {itemId: row.itemid})
    WITH v, i, row
    CALL {
        WITH v, i, row
        WITH v, i, row WHERE row.event = 'view'
        CREATE (v)-[:VIEWED {timestamp: row.timestamp}]->(i)
        UNION ALL
        WITH v, i, row
        WITH v, i, row WHERE row.event = 'addtocart'
        CREATE (v)-[:ADDED_TO_CART {timestamp: row.timestamp}]->(i)
        UNION ALL
        WITH v, i, row
        WITH v, i, row WHERE row.event = 'transaction'
        CREATE (v)-[:BOUGHT {timestamp: row.timestamp, transactionId: row.transactionid}]->(i)
    }
    """
    tx.run(cypher_query, batch=batch)

def main():
    args = parse_arguments()
    csv_file_path = os.path.join(args.source, "D:\SALSA'S STUDY\KULIAH\SEM 7\GRAF\data\events.csv")
    
    if not os.path.exists(csv_file_path):
        print(f"Error: File {csv_file_path} tidak ditemukan!")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(args.bolt, auth=(args.user, args.password))
        driver.verify_connectivity()
        print("Berhasil terhubung ke Neo4j!")
    except Exception as e:
        print(f"Gagal terhubung ke database: {e}")
        sys.exit(1)

    batch_size = 10000
    with driver.session() as session:
        create_constraints(session)
        print(f"Membaca file data dari: {csv_file_path}")
        chunks = pd.read_csv(csv_file_path, chunksize=batch_size)
        
        # Menghitung total baris untuk progress bar tqdm
        total_rows = sum(1 for _ in open(csv_file_path)) - 1
        total_chunks = (total_rows // batch_size) + 1

        print("Memulai proses impor data ke Neo4j...")
        for chunk in tqdm(chunks, total=total_chunks, desc="Mengimpor Batch Data"):
            chunk["transactionid"] = chunk["transactionid"].fillna("").astype(str)
            records = chunk.to_dict(orient="records")
            session.write_transaction(import_batch, records)

    driver.close()
    print("Proses impor berhasil selesai!")

if __name__ == "__main__":
    main()
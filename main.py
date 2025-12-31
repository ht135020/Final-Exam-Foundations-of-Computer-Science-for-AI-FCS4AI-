import requests

import pandas as pd

# Sends a request to "http://127.0.0.1:8000/comments" and retrieves the comments data
def fetch_data_from_api():
    response = requests.get("http://127.0.0.1:8000/comments")

    if response.status_code == 200:
        return response.json().get("comments", [])
    
    else:
        print("Failed to fetch data from API")
        return []

# ====================================================================
# Part A

comments = fetch_data_from_api()

df = pd.DataFrame(comments)

# ====================================================================
# Part B

filtered_emails_df = df[df['email'].str.endswith('.org')]

filtered_comments_df = filtered_emails_df[filtered_emails_df['body'].str.split().apply(len) > 20]

# ====================================================================
# Part C
filtered_comments_df = filtered_comments_df.copy()
filtered_comments_df["email"] = filtered_comments_df["email"].fillna("")
filtered_comments_df["body"] = filtered_comments_df["body"].fillna("")
filtered_comments_df["body"] = filtered_comments_df["body"].str.strip()
filtered_comments_df["body"] = filtered_comments_df["body"].str.lower()
filtered_comments_df["body"] = filtered_comments_df["body"].str.replace(r'[^\w\s]', '', regex=True)

# ====================================================================
# Part D

def chunk_by_words(text, max_words=30):
    words = text.split()
    chunks = []
    for i in range(0, len(words), max_words):
        chunk_words = words[i:i+max_words]
        chunks.append(" ".join(chunk_words))
    return chunks 

chunked_data = []

for _, row in filtered_comments_df.iterrows():
    comment_id = row['id']
    body = row['body']
    chunks = chunk_by_words(body, max_words=30)
    
    for chunk in chunks:
        chunked_data.append({
            "id": comment_id,
            "chunk": chunk
        })

chunked_df = pd.DataFrame(chunked_data)

# ====================================================================
# Part E

import sqlite3

conn = sqlite3.connect("chunks.db")

cursor = conn.cursor()

cursor.execute("""
              CREATE TABLE IF NOT EXISTS chunks (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               comment_id INTEGER,
               chunk_text TEXT
               )
               """)

conn.commit()

print("Database and table ready")

for _, row in chunked_df.iterrows():
    cursor.execute(
        "INSERT INTO chunks (comment_id, chunk_text) VALUES (?, ?)",
        (row["id"], row["chunk"])
    )

conn.commit()

conn.close()

print("All chunks inserted successfully")

# ====================================================================
# Part F

conn = sqlite3.connect("chunks.db")
cursor = conn.cursor()

cursor.execute("SELECT * FROM chunks WHERE chunk_text LIKE ?", ('%python%',))
rows = cursor.fetchall()

for row in rows:
    print(f"ID: {row[0]}, Comment ID: {row[1]}, Chunk Text: {row[2]}")

conn.close()

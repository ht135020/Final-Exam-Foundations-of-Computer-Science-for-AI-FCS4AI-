from fastapi import FastAPI

import csv

from pathlib import Path

app = FastAPI()

@app.get("/comments")
async def read_comments():
    path = Path("data/comments.csv")

    comments = []

    with path.open("r") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            comments.append(row)

    return {"comments": comments}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
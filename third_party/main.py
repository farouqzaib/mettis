from embeddings import SentenceTransformerEmbeddingsService

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Query(BaseModel):
    text : str

embeddingService = SentenceTransformerEmbeddingsService('msmarco-distilbert-base-v4')

@app.post("/embeddings")
async def generate_embeddings(query : Query):
    embedding = embeddingService.get_embedding(query.text)
    return {
        "status" : "success",
        "data" : embedding
    }
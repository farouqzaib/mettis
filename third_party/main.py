from sentence_transformers import SentenceTransformer, util
import torch
from argparse import ArgumentParser

embedder = SentenceTransformer("msmarco-distilbert-base-v4")

from fastapi import FastAPI
from pydantic import BaseModel
import json

app = FastAPI()

class Query(BaseModel):
    text : str

@app.post("/embeddings")
async def generate_embeddings(query : Query):
    embedding = embedder.encode(query.text)
    return {
        "status" : "success",
        "data" : embedding.tolist()
    }
from sentence_transformers import SentenceTransformer
import abc

class EmbeddingsService(abc.ABC):
    @abc.abstractmethod
    def get_embedding(text):
        pass

class SentenceTransformerEmbeddingsService(EmbeddingsService):
    def __init__(self, model_name):
        self.bi_encoder = SentenceTransformer(model_name)

    def get_embedding(self, text):
        return self.bi_encoder.encode(text).tolist()
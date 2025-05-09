from langchain.vectorstores.faiss import FAISS
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import LLMChainExtractor
from langchain.chat_models import ChatOpenAI

class RetrieverBuilder:
    def __init__(self, vectorstore: FAISS):
        self.vectorstore = vectorstore

    def build_basic_retriever(self):
        return self.vectorstore.as_retriever()

    def build_compression_retriever(self, llm_temperature: float = 0.0) -> ContextualCompressionRetriever:
        llm = ChatOpenAI(temperature=llm_temperature)
        compressor = LLMChainExtractor.from_llm(llm)
        return ContextualCompressionRetriever(
            base_compressor=compressor,
            base_retriever=self.vectorstore.as_retriever()
        )

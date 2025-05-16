from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document
import os

class VectorStoreManager:
    def __init__(self, persist_path="agent/vectorstore/faiss_index"):
        self.persist_path = persist_path
        self.embeddings = OpenAIEmbeddings()
        self.vectorstore = None

    def create_vectorstore(self, documents: list[Document]):
        """Create and persist a new FAISS vector store from a list of documents."""
        self.vectorstore = FAISS.from_documents(documents, self.embeddings)
        self.vectorstore.save_local(self.persist_path)

    def load_vectorstore(self):
        """Load a previously saved FAISS vector store."""
        if not os.path.exists(self.persist_path):
            raise FileNotFoundError("Vector store not found. Please run create_vectorstore() first.")
        self.vectorstore = FAISS.load_local(self.persist_path, self.embeddings)

    def load_vectorstore(self):
        """Safely load a previously saved FAISS vector store with format validation."""
        if not os.path.exists(self.persist_path):
            raise FileNotFoundError("Vector store not found. Please run create_vectorstore() first.")
        
        # Check expected file types in persist_path
        faiss_file = os.path.join(self.persist_path, "index.faiss")
        pkl_file = os.path.join(self.persist_path, "index.pkl")

        if not (os.path.exists(faiss_file) and os.path.exists(pkl_file)):
            raise ValueError("Expected .faiss and .pkl files not found in vectorstore path.")

        self.vectorstore = FAISS.load_local(
            self.persist_path,
            self.embeddings,
            allow_dangerous_deserialization=True
        )

    def add_documents(self, documents: list[Document]):
        """Add documents to the existing vector store and persist it."""
        if self.vectorstore is None:
            self.load_vectorstore()
        self.vectorstore.add_documents(documents)
        self.vectorstore.save_local(self.persist_path)

    def is_initialized(self) -> bool:
        """Check if the FAISS vectorstore has been initialized and saved."""
        return os.path.exists(os.path.join(self.persist_path, "index.faiss"))

    def similarity_search(self, query: str, k: int = 5) -> list[Document]:
        """Perform a similarity search on the vector store."""
        if self.vectorstore is None:
            self.load_vectorstore()
        return self.vectorstore.similarity_search(query, k=k)
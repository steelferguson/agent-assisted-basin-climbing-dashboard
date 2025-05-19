import unittest
from agent.insight_agent import InsightAgent
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.data_loader import initialize_data_uploader, load_df_from_s3
import data_pipeline.config as config


class TestInsightAgent(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        uploader = initialize_data_uploader()
        df = load_df_from_s3(uploader, config.aws_bucket_name, config.s3_path_combined)

        cls.vectorstore = VectorStoreManager(persist_path="agent/memory_store")
        cls.vectorstore.load_vectorstore()

        cls.memory = MemoryManager(vectorstore=cls.vectorstore)
        cls.agent = InsightAgent(vectorstore=cls.vectorstore, memory_manager=cls.memory)

        cls.agent.raw_transactions_df = df
        cls.df = df

    def test_summarize_overall_revenue(self):
        summary = self.agent.analyze_trends_and_generate_insights(
            start_date="2025-01-01", end_date="2025-01-07", category=None
        )
        self.assertIn("summary", summary.lower())
        print("Insight output:\n", summary)

    def test_summarize_specific_category(self):
        summary = self.agent.analyze_trends_and_generate_insights(
            start_date="2025-01-01", end_date="2025-01-07", category="Day Pass"
        )
        self.assertIn("day pass", summary.lower())
        print("Insight output:\n", summary)


if __name__ == "__main__":
    unittest.main()

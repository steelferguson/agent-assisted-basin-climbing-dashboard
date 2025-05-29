from agent.doc_generator import generate_timeperiod_summary_doc
import pandas as pd
from pydantic import BaseModel
from typing import Optional
from langchain_core.tools import StructuredTool


class SummaryInput(BaseModel):
    start_date: str
    end_date: str
    category: Optional[str] = None
    sub_category: Optional[str] = None


def generate_summary_document_with_df(df: pd.DataFrame):
    def inner(
        start_date: str,
        end_date: str,
        category: Optional[str] = None,
        sub_category: Optional[str] = None,
    ) -> str:
        doc = generate_timeperiod_summary_doc(
            df=df,
            start_date=start_date,
            end_date=end_date,
            category=category,
            sub_category=sub_category,
        )
        return doc.page_content

    return StructuredTool.from_function(
        name="generate_summary_document",
        func=inner,
        description="Generate a revenue summary for a given time period and category. Returns plain text.",
        args_schema=SummaryInput,
    )

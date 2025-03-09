import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
import plotly.graph_objects as go
import plotly.express as px


def get_project_root() -> str:
    """Returns project root folder."""
    db_path = f"{Path(__file__).parent.parent.parent}/.dbdir"
    Path(db_path).mkdir(parents=True, exist_ok=True)
    return f"{db_path}/duckdb_results.db"


class DuckDBReporter:

    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.data = None
        self.data_optimized = None

    def load_data(self, table_name: str = "duckdb_results.compare_result") -> pd.DataFrame:
        try:
            query = f"""
            with ct1 as (
                select 
                    process_engine,
                    dataset_size,                    
                    avg(cast(time_duration as decimal(10,2))) as avg_time_duration
                    from {table_name} as cr
                    group by process_engine, dataset_size 
            )
            select 
                process_engine,
                dataset_size,
                avg_time_duration as avg_duration_sec
            from ct1
            order by process_engine
            """

            return self.conn.execute(query).fetchdf()
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None

    def create_chart(self, x_column: str, y_column: str,
                     height: int = 500,
                     width: int = 800):
        """
        Create a Bar chart using the loaded data.
        """
        if self.data is None:
            st.error("No data loaded. Please call load_data() first.")
            return None

        # Create a combined figure directly
        figlayout_fig = go.Figure()

        # Add bar traces for regular data
        figlayout_fig.add_trace(go.Bar(
            x=self.data[x_column],
            y=self.data[y_column],
            name='Regular',
            marker_color='blue'
        ))

        # Add bar traces for optimized data
        figlayout_fig.add_trace(go.Bar(
            x=self.data_optimized[x_column],
            y=self.data_optimized[y_column],
            name='Optimized',
            marker_color='green'
        ))

        # Update layout
        figlayout_fig.update_layout(
            title='Performance Comparison: Regular vs Optimized',
            xaxis_title=x_column,
            yaxis_title=y_column,
            barmode='group',
            height=height,
            width=width,
            legend_title="Version"
        )

        # Display the chart
        st.plotly_chart(figlayout_fig, use_container_width=True)

        return figlayout_fig

    def run_report(self, x_column: str, y_columns: List[str],
                   title: str = "Data Visualization Report"):
        st.title(title)

        # Load data
        with st.spinner("Loading data..."):
            self.data = self.load_data()
            self.data_optimized = self.load_data(
                table_name="duckdb_results.compare_result_optimized")
            st.success(f"Loaded {len(self.data)} records from {'xx'}")

        # Create visualization
        st.subheader(
            "Performance Chart Visualization with 100 million records")
        self.create_chart(x_column, y_columns)


# Example usage
if __name__ == "__main__":
    st.set_page_config(page_title="Performance Data Report", layout="wide")

    db_path = f"{get_project_root()}"
    print(f"DB Path: {db_path}")

    reporter = DuckDBReporter(
        db_path=db_path)

    reporter.run_report(
        x_column="process_engine",
        y_columns="avg_duration_sec"
    )

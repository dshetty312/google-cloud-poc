import streamlit as st
import pandas as pd
import plotly.express as px
import google.cloud.bigquery as bigquery
from google.cloud import aiplatform
import vertexai
from vertexai.language_models import TextGenerationModel
import json
from typing import Dict, List
import os

# Initialize Vertex AI
vertexai.init(project="your-project-id", location="your-location")

class GeminiBigQueryBot:
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.model = TextGenerationModel.from_pretrained("gemini-pro")
        self.chat_history = []
        
    def get_table_schema(self, table_id: str) -> str:
        """Get the schema of a BigQuery table"""
        table = self.bq_client.get_table(table_id)
        schema_info = "\nTable Schema:\n"
        for field in table.schema:
            schema_info += f"- {field.name} ({field.field_type})\n"
        return schema_info

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a BigQuery query and return results as DataFrame"""
        try:
            return self.bq_client.query(query).to_dataframe()
        except Exception as e:
            st.error(f"Query execution failed: {str(e)}")
            return None

    def generate_visualization(self, df: pd.DataFrame, viz_type: str, x_col: str, y_col: str, title: str):
        """Generate visualization using Plotly"""
        try:
            if viz_type == "bar":
                fig = px.bar(df, x=x_col, y=y_col, title=title)
            elif viz_type == "line":
                fig = px.line(df, x=x_col, y=y_col, title=title)
            elif viz_type == "scatter":
                fig = px.scatter(df, x=x_col, y=y_col, title=title)
            elif viz_type == "pie":
                fig = px.pie(df, values=y_col, names=x_col, title=title)
            st.plotly_chart(fig)
        except Exception as e:
            st.error(f"Visualization failed: {str(e)}")

    def process_user_query(self, user_query: str, table_id: str) -> str:
        """Process user query using Gemini"""
        # Get table schema for context
        schema_info = self.get_table_schema(table_id)
        
        # Construct prompt with context
        prompt = f"""
        You are a helpful AI assistant that helps users analyze BigQuery data.
        
        Table Information:
        {schema_info}
        
        User Question: {user_query}
        
        Please provide:
        1. A BigQuery SQL query to answer the question
        2. An explanation of the analysis
        3. Visualization suggestions if applicable
        
        Format your response as JSON with keys: 'query', 'explanation', 'visualization'
        """
        
        # Get response from Gemini
        response = self.model.predict(prompt)
        
        try:
            # Parse the response as JSON
            response_data = json.loads(response.text)
            return response_data
        except json.JSONDecodeError:
            st.error("Failed to parse Gemini response")
            return None

def main():
    st.title("💬 Gemini BigQuery Chatbot")
    
    # Initialize bot
    if 'bot' not in st.session_state:
        st.session_state.bot = GeminiBigQueryBot()
    
    # Sidebar for configuration
    st.sidebar.header("Configuration")
    table_id = st.sidebar.text_input(
        "BigQuery Table ID",
        "project.dataset.table"
    )
    
    # Main chat interface
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask me about your data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Process query and get response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response_data = st.session_state.bot.process_user_query(prompt, table_id)
                
                if response_data:
                    # Display explanation
                    st.markdown(response_data["explanation"])
                    
                    # Execute query and show results
                    if "query" in response_data:
                        st.code(response_data["query"], language="sql")
                        df = st.session_state.bot.execute_query(response_data["query"])
                        
                        if df is not None:
                            st.dataframe(df)
                            
                            # Generate visualization if suggested
                            if "visualization" in response_data and response_data["visualization"]:
                                viz_info = response_data["visualization"]
                                st.session_state.bot.generate_visualization(
                                    df,
                                    viz_info.get("type", "bar"),
                                    viz_info.get("x_column"),
                                    viz_info.get("y_column"),
                                    viz_info.get("title", "Data Visualization")
                                )
                
                # Add assistant response to chat history
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"{response_data['explanation']}\n```sql\n{response_data['query']}\n```"
                })

if __name__ == "__main__":
    main()

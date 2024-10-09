import os
import io

import pandas as pd
import streamlit as st
import snowflake.connector

from gpt import OpenAIService

st.set_page_config(layout="wide")


# Custom CSS for styling
st.markdown(
    """
    <style>
    body {
        background-color: #4169E1;  /* Light cyan background */
    }
    .sidebar .sidebar-content {
        background-color: #4682B4;  /* White sidebar background */
    }
    .stTextInput, .stTextArea {
        background-color: #B2BEB5; /* White input field background */
        color: #333333; /* Dark text in input fields for contrast */
        border-radius: 15px; /* Rounded corners */
        border: 2px solid #00bcd4; /* Bright border color */
        margin: 10px 0; /* Vertical margin for spacing */
        padding: 10px; /* Padding for better input area */
    }
    .stTextInput label, .stTextArea label {
        margin-bottom: 5px; /* Reduce space between label and input */
    }
    .stButton {
        background-color: #00bcd4; /* Bright button background */
        color: #ffffff; /* White text on buttons */
        border: 2px solid #0097a7; /* Slightly darker border color for buttons */
        border-radius: 15px; /* Rounded corners for buttons */
        margin: 10px 0; /* Vertical margin for spacing */
    }
    .stButton:hover {
        background-color: #0097a7; /* Darker button hover color */
    }
    h1, h2, h3, h4, h5, h6 {
        color: #006064; /* Dark teal headers */
    }
    .stPasswordInput .stTextInput div:last-child {
    display: none; /* Hides the password toggle icon */
    }
    [title="Show password text"] {
        display: none;
    }
    """,
    unsafe_allow_html=True,
)



with st.sidebar:
    st.caption("Snowflake Credentials")
    os.environ["SNOWFLAKE_USER"] = st.text_input("User", value="siri")
    # os.environ["SNOWFLAKE_PASSWORD"] = st.text_input("Password", value="Techment@123", placeholder="Enter your password")
    os.environ["SNOWFLAKE_PASSWORD"] = st.text_input("Password", value="Techment@123",key="hidden_password", type="password")
    os.environ["SNOWFLAKE_ACCOUNT"] = st.text_input("Account", value="jv51685.central-india.azure")
    os.environ["SNOWFLAKE_WAREHOUSE"] = st.text_input("Warehouse", value="COMPUTE_WH")
    os.environ["SNOWFLAKE_SCHEMA"] = st.text_input("Schema", value="RAW")
    os.environ["SNOWFLAKE_DATABASE"] = st.text_input("Database", value="SALES_REVENUE_DATA")



    st.write("---")
    api_key = st.text_input("OpenAI API Key", type="password")
    if api_key:
        os.environ["OPENAI_API_KEY"] = api_key
    else:
        st.info("Please enter OpenAI API Key")
        st.stop()

creds = {
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
}

@st.cache_resource
class SnowflakeDB:
    def __init__(self) -> None:
        self.conn = snowflake.connector.connect(**creds)
        self.cursor = self.conn.cursor()

def read_prompt_file(fname):
    with open(fname, "r") as f:
        return f.read()

@st.cache_data
def query(_conn, query):
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        st.warning("Error in query")
        st.error(e)

@st.cache_data
def ask(prompt):
    response = gpt.prompt(prompt)
    return response["choices"][0]["message"]["content"]

def get_tables_schema(_conn):
    with st.expander("View tables schema in database"):
        table_schemas = ""
        df = query(_conn, "show tables")
        st.write(df)
        for table in df["name"]:
            t = f"{DATABASE_NAME}.{SCHEMA_NAME}.{table}"
            df = query(sf.conn, f"select * from {t} limit 10;")
            ddl_query = f"select get_ddl('table', '{t}');"
            ddl = query(sf.conn, ddl_query)
            schema = f"\n{ddl.iloc[0, 0]}\n"
            st.write(f"### {table}")
            st.markdown(f"```sql{schema}```")
            st.write("---")

            table_schemas += f"\n{table}\n{schema}\n"

    return table_schemas

def get_sample_questions(table_schemas):
    with st.expander("Sample Questions"):
        prompt = read_prompt_file("sample_questions_prompt.txt")
        prompt = prompt.replace("<<TABLES>>", table_schemas)
        answer = ask(prompt)
        st.code(answer)

def df_schema(df):
    # -- data schema
    sio = io.StringIO()
    df.info(buf=sio)
    df_info = sio.getvalue()
    return df_info

def validate_sql(sql):
    # List of restricted keywords
    restricted_keywords = ["DROP", "ALTER", "TRUNCATE", "UPDATE", "REMOVE"]
    
    # Check if any restricted keywords are in the SQL query
    for keyword in restricted_keywords:
        if keyword in sql.upper():
            return False, keyword
    return True, None

if __name__ == "__main__":
    st.title("Snowflake-Streamlit")
    msg = "Connect to your Snowflake database and ask questions about your data and get answers in real-time with visualization supported."
    st.write(msg)

    gpt = OpenAIService()
    sf = SnowflakeDB()

    SCHEMA_NAME = os.environ["SNOWFLAKE_SCHEMA"]
    DATABASE_NAME = os.environ["SNOWFLAKE_DATABASE"]
    WAREHOUSE_NAME = os.environ["SNOWFLAKE_WAREHOUSE"]

    # -- get tables DDL in schema
    table_schemas = get_tables_schema(sf.conn)

    # -- sample questions
    sample_questions = get_sample_questions(table_schemas)

    st.write("---")

    # -- ask SQL question
    question = st.text_area(
        "Hi! I’m Snow Chat, your data assistant. Ask me anything about your connected data, and let’s find insights together!",
        placeholder="What is the total revenue?",
        height=100,  
    )


    
    if question:
        # -- curate prompt
        prompt = read_prompt_file("sql_prompt.txt")
        prompt = prompt.replace("<<TABLES>>", table_schemas)
        prompt = prompt.replace("<<QUESTION>>", question)
        answer = ask(prompt)

        # Remove any Markdown formatting
        answer = answer.replace("```sql", "").replace("```", "").strip()


        # Validate the SQL query before execution
        is_valid, keyword = validate_sql(answer)
        if not is_valid:
            st.error(f"Operation not allowed: {keyword} is restricted!")
            st.stop()
        
        st.code(answer)

        # -- Execute the query if valid
        df = query(sf.conn, answer)
        st.dataframe(df, use_container_width=True)
    else:
        st.stop()


 # -- ask Python question
    question = st.text_input(
        "Ask a question about the result",
        placeholder="e.g. Visualize the data",
    )
    if question:
        # -- curate prompt
        df_info = df_schema(df)

        prompt = read_prompt_file("python_prompt.txt")
        prompt = prompt.replace("<<DATAFRAME>>", df_info)
        prompt = prompt.replace("<<QUESTION>>", question)
        answer = ask(prompt)
        with st.expander("view generated code"):
            st.code(answer)
        answer = answer.replace("```python", "") # hotfix
        answer = answer.replace("```", "")
        exec(answer)
This paste expires in <1 hour. Public IP access. Share whatever you see with others in seconds with Context.Terms of ServiceReport this

import os
import json
import openai
import sqlite3
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure OpenAI API key from environment variables
openai.api_key = os.getenv("OPENAI_API_KEY")

# Validate API key existence
if not openai.api_key:
    raise ValueError("OpenAI API key not found in environment variables")

# Load CSV file
csv_file = "/content/Example2_data_pyspark.csv"
df = pd.read_csv(csv_file)

# Create SQLite database
db_conn = sqlite3.connect("Employee Database.db")
cursor = db_conn.cursor()

def list_tables() -> list[str]:
    """Retrieve the names of all tables in the database."""
    # Include print logging statements so you can see when functions are being called.
    print(' - DB CALL: list_tables')

    cursor = db_conn.cursor()

    # Fetch the table names.
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    tables = cursor.fetchall()
    return [t[0] for t in tables]

def describe_table(table_name: str) -> list[tuple[str, str]]:
    """Look up the table schema.

    Returns:
      List of columns, where each entry is a tuple of (column, type).
    """
    print(' - DB CALL: describe_table')

    cursor = db_conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name});")

    schema = cursor.fetchall()
    # [column index, column name, column type, ...]
    return [(col[1], col[2]) for col in schema]

def execute_query(sql: str) -> list[list[str]]:
    """Execute a SELECT statement, returning the results."""
    print(' - DB CALL: execute_query')

    cursor = db_conn.cursor()

    cursor.execute(sql)
    return cursor.fetchall()

def initialize_database():
    """Initialize the database with CSV data if it doesn't exist or is empty."""
    print("Checking database status...")

    # Check if table exists and has data
    cursor = db_conn.cursor()
    tables = list_tables()

    if "employee" not in tables:
        print("Creating new employee table from CSV...")
        df = pd.read_csv(csv_file)  # Read CSV file again to ensure fresh data
        df.to_sql("employee", db_conn, if_exists="fail", index=False)
        print("Database initialized successfully")
    else:
        # Check if table is empty
        cursor.execute("SELECT COUNT(*) FROM employee")
        count = cursor.fetchone()[0]

        if count == 0:
            print("Table exists but is empty. Loading CSV data...")
            df = pd.read_csv(csv_file)  # Read CSV file again to ensure fresh data
            df.to_sql("employee", db_conn, if_exists="append", index=False)
            print("Data loaded successfully")
        else:
            print(f"Using existing database with {count} records")

# Update the system instruction
instruction = """You are a helpful chatbot that can interact with an SQL database for an employee database.
You will take the users questions and turn them into SQL queries using the tools available.
You can only:
1. Query data using SELECT statements
2. Retrieve table schemas
3. List available tables

First use list_tables to see available tables, and describe_table to understand the schema.
Then you can execute_query for SELECT operations."""

def process_request(user_query: str):
    tools = {
        "list_tables": lambda args: list_tables(),
        "describe_table": lambda args: describe_table(args.get("table_name")),
        "execute_query": lambda args: execute_query(args.get("query"))
    }

    messages = [
        {"role": "system", "content": instruction},
        {"role": "user", "content": user_query}
    ]

    while True:
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "list_tables",
                            "description": "List all tables in the database",
                            "parameters": {
                                "type": "object",
                                "properties": {}
                            }
                        }
                    },
                    {
                        "type": "function",
                        "function": {
                            "name": "describe_table",
                            "description": "Get the schema of a specific table",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "table_name": {
                                        "type": "string",
                                        "description": "Name of the table to describe"
                                    }
                                },
                                "required": ["table_name"]
                            }
                        }
                    },
                    {
                        "type": "function",
                        "function": {
                            "name": "execute_query",
                            "description": "Execute a SQL SELECT query",
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "query": {
                                        "type": "string",
                                        "description": "SQL SELECT query to execute"
                                    }
                                },
                                "required": ["query"]
                            }
                        }
                    }
                ],
                tool_choice="auto"
            )

            message = response.choices[0].message

            if message.content:
                print(message.content)

            if not message.tool_calls:
                break

            for tool_call in message.tool_calls:
                function_name = tool_call.function.name
                try:
                    arguments = json.loads(tool_call.function.arguments)

                    if function_name in tools:
                        function_result = tools[function_name](arguments)

                        messages.append({
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [tool_call]
                        })
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "name": function_name,
                            "content": json.dumps(function_result)
                        })
                    else:
                        print(f"Unknown function: {function_name}")
                except json.JSONDecodeError:
                    print(f"Error parsing arguments for {function_name}")
                except Exception as e:
                    print(f"Error executing {function_name}: {str(e)}")

        except Exception as e:
            print(f"Error in API call: {str(e)}")
            break

def interactive_database_session():
    """Start an interactive database session that continues until user ends it."""
    print("Database Interaction Session Started")
    print("Type 'End this Session' to quit")
    while True:
        try:
            # Get user input
            user_query = input("Enter your query (or 'End this Session'): ")
            # Check for session end
            if user_query.lower() == 'end this session':
                print("Database Session Ended. Goodbye!")
                break
            # Process the query
            process_request(user_query)
        except KeyboardInterrupt:
            print("\nSession interrupted. Ending database interaction.")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

# Initialize database
initialize_database()

# Start interactive session
interactive_database_session()
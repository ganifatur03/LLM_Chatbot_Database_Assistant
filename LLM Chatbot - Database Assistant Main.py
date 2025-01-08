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

# Membuat sambungan ke sqlite dan membuat database
db_file = 'example.db'
db_conn = sqlite3.connect(db_file)
cursor = db_conn.cursor()

# Menyimpan dan membaca file CSV yang akan ddimasukkan ke db
csv_file = r"C:/Users/sprat/Documents/example_data_employee.csv"
df = pd.read_csv(csv_file)

def list_tables() -> list[str]:
    """Retrieve the names of all tables in the database."""
    # Include print logging statements so you can see when functions are being called.
    print(' - DB CALL: list_tables')

    cursor = db_conn.cursor()

    # Fetch the table names.
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")

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

# Drop tabel kalau perlu
#drop_table = """DROP TABLE IF EXISTS (nama tabel)"""
#cursor.execute(drop_table)

# Membuat tabel (CREATE TABLE kalau belum ada nama_tabel(kolom1, kolom 2, kolom seterusnya))
create_table = '''CREATE TABLE IF NOT EXISTS DummyEmployee(
                employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                contact_number INTEGER NOT NULL,
                gender TEXT NOT NULL,
                age INTEGER NOT NULL,
                address TEXT NOT NULL,
                job TEXT NOT NULL,
                salary INTEGER NOT NULL);
                '''
# Menjalankan membuat table
cursor.execute(create_table)

# Memasukkan data dari file CSV ke SQLite (nama tabel, koneksi ke tabel, jika_tabel_ada, setting_index)
df.to_sql("DummyEmployee", db_conn, if_exists='replace',index=False)

# Kode kalau mau ngecek isi tabel yang sudah dibuat
"""cursor.execute('SELECT * FROM DummyEmployee')
rows = cursor.fetchall()
cursor.close()
for row in rows:
   print(row)"""

# Buat instruksi untuk AI Chatbox
instruction = """You are a helpful chatbox that can interact with SQL database for a company computer. You will take users questions 
and turn them into SQL queries using tools available. Once you have information you need, you will answer the user's question using
the data returned. Use list_tables to see what tables are present, describe_table to understand the schema, and execute_query to
issue a SQL SELECT query. All queries should consider case insensitivity. Always convert text inputs to lowercase for comparison."""

# Membuat penjelasan untuk tools chatbox untuk dimengerti oleh OpenAI
chatbox_tools= [
    {
        "type": "function",
        "function": {
            "name": "list_tables",
            "description": "Get the list of all tables in the database",
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
]

def process_request(user_query: str):
    tools = {
        "list_tables": lambda args: list_tables(),
        "describe_table": lambda args: describe_table(args.get("table_name")),
        "execute_query": lambda args: execute_query(args.get("query"))
    }

    # Menyatakan role dan konten untuk nanti digunakan di prompt AI
    messages = [
        {
            "role": "system",
            "content": instruction
        },
        {
            "role": "user",
            "content": user_query
        }
    ]

    while True:
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=messages,
                tools=chatbox_tools
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

# Start interactive session
interactive_database_session()

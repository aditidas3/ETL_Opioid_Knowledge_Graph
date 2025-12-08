# Opioid Knowledge Graph

This is a knowledge graph built from documents regarding the opioid epidemic with the goal of connecting main actors and events to illustrate compliance and inform policymakers towards drug legislation. The document data is hosted by UCSF, so a VPN connection to the database needs to be established prior to code execution. 

## Setup

There are some strings like the database query and Solr URL that we're not sure should be in a public repository (honestly also not sure how to program besides as a raw string). With permission we can add these back into our code. 

**Packages**

These are the Python packages I installed as first order of business. 

`pip install "psycopg[binary]" openai aiofiles neo4j load_dotenv`

**Environment variables**
* DB_HOST
* DB_NAME
* DB_USER
* DB_PASS
* OPENAI_KEY
* OPENAI_MODEL
* NEO4J_URI   (*"neo4j://localhost:7687" for local instance*)
* NEO4J_USER  (*"neo4j" for local instance*)
* NEO4J_PASS

## Additional Files
1. [INPUT] `qwen_output_v1.jsonl` (output from Qwen API)
2. [OUTPUT] The database query results will be created and stored as `email_bodies_list.csv` in your local directory. 
3. [OUTPUT] The Batch API requires creation of a few JSONL files to run. They will appear as `batch_####.jsonl` in your local directory after code execution
4. [OUTPUT] The full output of the OpenAI API is stored as `all_emails_structured.jsonl`

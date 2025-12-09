# Opioid Knowledge Graph

This project builds an end-to-end ETL pipeline that transforms raw opioid-related documents into a structured, enriched, and connected knowledge graph. The system is designed to help researchers and policymakers explore relationships between entities, events, and actors in the opioid epidemic. The document data is hosted by UCSF, so a VPN connection to the database needs to be established prior to code execution.

## Project Execution

### **Part 1 - Data Extraction & JSON Generation**

* Connect to the UCSF-hosted SQL database (VPN required).
* Extract raw email bodies from Solr (VPN required). 
* Use **OpenAI LLM** to convert unstructured text into structured JSON objects.
* Output standardized JSONL files ready for downstream enrichment.

### **Part 2 - Semantic Enrichment**

* Use **spaCy biomedical models** to identify chemical names and medically relevant terms.
* Map extracted terms to **RxNorm** to obtain RXCUI identifiers and normalized drug names.
* Applie **Qwen LLM** to add enriched semantic information like decisions, concerns, events, location, financials, people.
* Produce semantically enhanced JSON ready for entity resolution.

### **Part 3 - Entity Resolution & Graph Construction**

* Import all data from JSONL (creates duplicates)
* Preview duplicate entities
* Resolve duplicates by merging similar entities
* Report statistics
* Load the refined entities into nodes and edges in **Neo4j** to construct the knowledge graph

## Setup

Database tables, connection strings, Solr URL aren't publicly exposed. Add them locally through environment variables.

### **Required Packages**

Install the primary dependencies:

```bash
pip install "psycopg[binary]" openai aiofiles neo4j python-dotenv
pip install "spacy>=3.7.0,<3.8.0" "scispacy>=0.5.3,<0.6.0"
pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.4/en_ner_bc5cdr_md-0.5.4.tar.gz
```

### **Environment Variables**

| Variable       | Description                                 |
| -------------- | ------------------------------------------- |
| `DB_HOST`      | SQL database host                           |
| `DB_NAME`      | Database name                               |
| `DB_USER`      | Username                                    |
| `DB_PASS`      | Password                                    |
| `OPENAI_KEY`   | API key for OpenAI                         |
| `OPENAI_MODEL` | Model used for OpenAI                      |
| `NEO4J_URI`    | `"neo4j://localhost:7687"` for local instance |
| `NEO4J_USER`   | `"neo4j"` for local instance                |
| `NEO4J_PASS`   | Neo4j password                              |
| `QWEN_API`     | API key for Qwen                            |
| `QWEN_MODEL`   | Model used for Qwen                         |



## Output Files

* `email_bodies_list.csv` - output of all ids and their email body text after extracting it from Solr
* `all_emails_structured.jsonl` - output after OpenAI process to get structured JSON
* `json_with_crossRefs.jsonl` - output after adding cross reference ids to JSONL
* `json_with_crossRefs_rxnorm.jsonl` - output after adding RxNorm matched drugs names to JSONL
* `enriched_output.jsonl` - final output after Qwen API process to get enriched JSON





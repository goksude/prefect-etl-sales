Markdown

# Prefect 3 ELT Sales Pipeline 

This project sets up a fully automated ELT (Extract, Load, Transform) pipeline using Prefect 3, Docker, and PostgreSQL.

The pipeline fetches a sales dataset from the Kaggle API, loads the raw data into a PostgreSQL database, and then cleans and transforms the data *inside* the database using a stored procedure.

---

## 🛠️ Tech Stack

* **Orchestration:** Prefect 3
* **Containerization:** Docker & Docker Compose
* **Database:** PostgreSQL
* **Language:** Python 3.9+
* **Core Libraries:** `pandas`, `sqlalchemy`, `kaggle`

---

## 📂 Project Structure

The project is structured to allow all services (`prefect`, `worker`, `database`) to be launched with a single command.

```bash
prefect_setup/                  # Main project directory
├── db_init_scripts/            # Database initialization scripts
│   ├── 01_create_clean_table.sql # 1st SQL: Creates the clean table
│   └── 02_create_stored_procedure.sql # 2nd SQL: Creates the transformation procedure
├── env_example                # Template for the .env file
├── compose.yaml                # Defines all services (prefect, worker, db)
├── Dockerfile                  # Builds the custom Docker image for the prefect worker
├── prefect.yaml                # Prefect 3 deployment configuration
├── prefect_flow.py             # Main ELT flow logic (E-L-T tasks)
└── requirements.txt            # Python dependencies
```
🚀 Getting Started
You must have Docker Desktop installed and running to use this project locally.

1. Clone the Project

```bash git clone [YOUR_PROJECT_GIT_URL]
cd prefect_setup
```

2. Configure Environment Variables

The project needs "secrets" to connect to the Kaggle API and the database. Copy the .env.example file and fill it in.
```Bash
cp env_example .env
```
Now, open the .env file and fill in the following fields:

```
# === Kaggle API ===
# Get from your Kaggle Account (Settings > API > Create New API Token)
KAGGLE_USERNAME=YOUR_KAGGLE_USERNAME
KAGGLE_KEY=YOUR_KAGGLE_API_TOKEN

# === PostgreSQL Database ===
# These values must match the 'sales-db' service in compose.yaml
ELT_CONNECTION_STRING="postgresql://<YOUR_DB_USER>:<YOUR_DB_PASSWORD>@sales-db:5432/<YOUR_DB_NAME>"
```
Important: Do not change the sales-db hostname in the ELT_CONNECTION_STRING. This is the internal network name Docker uses to let the services communicate.

3. Start the Services

You can now start all services (Prefect UI, worker, and database) with a single command.

```Bash

docker-compose up --build -d
```
--build: Rebuilds the image if you made changes to the Dockerfile.

-d: Runs the services in the background (detached mode) and returns your terminal to you.

🛰️ Deploying and Running the Flow
The services are up, but the Prefect server doesn't know about your flow yet. We need to register it.

1. Deploy the Flow

The following command enters the running prefect-worker container, finds the prefect.yaml file, and applies it to the server.

```Bash

docker-compose exec -w /app prefect-worker prefect deploy
```
2. Run from the UI

Open the Prefect UI: http://localhost:4200

Go to the Deployments tab from the menu on the left.

Find sales-elt-flow (or the name you set in prefect.yaml).

Click the Run button in the top right to trigger the flow manually.

You can now watch the flow execute and see its logs live in the UI.

🛑 Stopping and Cleaning Up

When you're finished, stop all services:

```Bash

docker-compose down
```
⚠️ Resetting the Project (Deleting the Database)
If you want to start over from scratch and permanently delete all data in the database (the volumes), use the -v flag:

```Bash

docker-compose down -v
```
📄 License 

This project is under the MIT License.
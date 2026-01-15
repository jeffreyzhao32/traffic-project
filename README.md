# Road Safety Events Data Pipeline

A comprehensive data engineering project for ingesting, processing, and visualizing road safety events data using Apache Airflow, PostgreSQL (Supabase), and Streamlit.

## 🎯 Project Overview

This project provides an end-to-end data pipeline for road safety events:

1. **Data Ingestion**: Automated daily ingestion of road safety events from ArcGIS REST API
2. **Data Storage**: PostgreSQL database hosted on Supabase
3. **Data Orchestration**: Apache Airflow for workflow management and scheduling
4. **Data Visualization**: Interactive Streamlit dashboard for analysis and exploration

## 📁 Project Structure

```
traffic-project/
├── dags/                          # Apache Airflow DAGs
│   ├── traffic_ingestion_dag.py   # Main ETL pipeline for road safety data
│   ├── test_db_connection_dag.py  # Database connection testing DAG
│   └── explore_database_dag.py    # Database exploration DAG
├── scripts/                       # Utility scripts
│   ├── streamlit_app.py          # Streamlit dashboard application
│   └── db_reader.py              # Database exploration script
├── sql/                          # SQL analysis queries
│   ├── road_safety_analysis.sql  # Comprehensive analysis queries
│   ├── data_exploration.sql      # Data exploration queries
│   └── column_updates.sql        # Schema update queries
├── data/                         # Raw data files
│   └── Road_Safety.csv          # Sample data file
├── config/                       # Configuration files
│   └── airflow.cfg              # Airflow configuration
├── docker/                       # Docker configuration
│   └── Dockerfile/              # Docker build files
├── tests/                        # Test files
├── docker-compose.yaml          # Docker Compose configuration for Airflow
├── requirements.txt             # Python dependencies for Streamlit app
└── README.md                    # This file
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- PostgreSQL client (optional, for direct database access)
- Supabase account with database connection details

### Setup

#### 1. Clone the Repository

```bash
git clone <repository-url>
cd traffic-project
```

#### 2. Set Up Virtual Environment

```bash
# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate

# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

#### 3. Configure Environment Variables

Create a `.env` file in the root directory (this file is gitignored):

```bash
# Database connection (for Streamlit and scripts)
DATABASE_URL=postgresql://user:password@host:port/database

# Airflow configuration (if needed)
AIRFLOW_UID=$(id -u)
```

**Note**: The `DATABASE_URL` should match your Supabase connection string.

#### 4. Set Up Airflow (Docker)

```bash
# Initialize Airflow
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Access Airflow UI at http://localhost:8080
# Default credentials: airflow / airflow
```

#### 5. Configure Airflow Connection

In the Airflow UI:
1. Go to **Admin** → **Connections**
2. Add a new connection:
   - **Connection Id**: `supabase_db`
   - **Connection Type**: `Postgres`
   - **Host**: Your Supabase host
   - **Schema**: `postgres`
   - **Login**: Your Supabase user
   - **Password**: Your Supabase password
   - **Port**: `5432`

Alternatively, use the Airflow CLI:

```bash
docker-compose run airflow-cli connections add supabase_db \
    --conn-type postgres \
    --conn-host <your-supabase-host> \
    --conn-schema postgres \
    --conn-login <your-username> \
    --conn-password <your-password> \
    --conn-port 5432
```

## 📊 Data Pipeline

### Traffic Ingestion DAG

The main data pipeline (`traffic_ingestion_dag.py`) runs daily and:

1. **Fetches Data**: Retrieves road safety events from ArcGIS REST API (last 5 days)
2. **Transforms Data**: Extracts and parses GeoJSON features
3. **Loads Data**: Inserts/updates records in PostgreSQL database using upsert logic

**Key Features**:
- Daily scheduled execution (`@daily`)
- Handles duplicates using `ON CONFLICT` with primary key
- Extracts geographic coordinates (x, y)
- Parses datetime fields
- Error handling and logging

### Database Schema

The `road_safety_events` table contains:

| Column | Type | Description |
|--------|------|-------------|
| `unique_identifier` | TEXT (PK) | Unique event identifier |
| `occurrence_date` | TIMESTAMP | Date/time of occurrence |
| `occurrence_detail` | TEXT | Details about the occurrence |
| `location_code` | TEXT | Location classification |
| `municipality` | TEXT | Municipality name |
| `involve_drug_or_alcohol` | TEXT | Drug/alcohol involvement |
| `road_safety_occurrence_type` | TEXT | Type of road safety event |
| `x` | DOUBLE PRECISION | Longitude coordinate |
| `y` | DOUBLE PRECISION | Latitude coordinate |
| `time_est` | TIME | Estimated time of occurrence |

## 🎨 Streamlit Dashboard

### Running the Dashboard

```bash
# Activate virtual environment
source .venv/bin/activate  # On macOS/Linux

# Set database connection
export DATABASE_URL="postgresql://user:password@host:port/database"

# Run Streamlit app
streamlit run scripts/streamlit_app.py
```

The dashboard will open at `http://localhost:8501`

### Dashboard Features

- **Overview Metrics**: Total events, date range, municipality count
- **By Municipality**: Distribution of events across municipalities
- **By Event Type**: Breakdown of different road safety occurrence types
- **Drug/Alcohol Involvement**: Analysis of substance-related events
- **Temporal Analysis**: Time-based patterns (day of week, hour of day, timeline)
- **Geographic Visualization**: Interactive maps using Folium to visualize event locations
- **Raw Data Browser**: Interactive data table with download option

## 📝 SQL Analysis

The `sql/road_safety_analysis.sql` file contains comprehensive analysis queries:

- Overview statistics
- Events by municipality, type, and location
- Drug/alcohol involvement analysis
- Temporal patterns (daily, hourly, by day of week)
- Geographic analysis
- Data quality checks

Run queries directly in your database client or use the `db_reader.py` script:

```bash
python scripts/db_reader.py
```

## 🛠️ Development

### Running DAGs

DAGs are automatically loaded by Airflow. To trigger manually:

1. Open Airflow UI at `http://localhost:8080`
2. Find your DAG in the list
3. Toggle it ON if paused
4. Click the play button to trigger a run

### Testing Database Connection

Use the test DAG or run directly:

```bash
python scripts/db_reader.py
```

### Adding New DAGs

1. Create a new Python file in the `dags/` directory
2. Follow the Airflow DAG structure
3. The DAG will automatically appear in the Airflow UI after a few seconds

## 🔧 Troubleshooting

### Airflow Issues

```bash
# Check Airflow logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# Restart services
docker-compose restart

# Reset Airflow (WARNING: Deletes data)
docker-compose down -v
docker-compose up airflow-init
```

### Database Connection Issues

- Verify connection string in `.env` file
- Check Supabase connection settings
- Ensure database credentials are correct
- Verify network access to Supabase host

### Streamlit Issues

- Ensure virtual environment is activated
- Verify `DATABASE_URL` environment variable is set
- Check that all dependencies are installed: `pip install -r requirements.txt`

## 📦 Dependencies

### Python Packages (requirements.txt)
- `streamlit>=1.28.0` - Web dashboard framework
- `pandas>=2.0.0` - Data manipulation
- `plotly>=5.17.0` - Interactive visualizations and charts
- `psycopg2-binary>=2.9.9` - PostgreSQL adapter
- `folium>=0.15.0` - Interactive maps and geographic visualizations

### Airflow Dependencies
Managed via Docker image: `apache/airflow:3.1.0`
- Additional providers for PostgreSQL, HTTP requests
- Installed automatically in the Docker container

## 📈 Data Sources

- **ArcGIS REST API**: Road Safety Feature Service
  - URL: `https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/Road_Safety/FeatureServer/0/query`
  - Updates: Daily ingestion of events from the last 5 days

## 🤝 Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly
4. Submit a pull request


## 👤 Author

Jeffrey Zhao


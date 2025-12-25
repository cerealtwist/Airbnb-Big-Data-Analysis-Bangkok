# Scalability Analysis of Big Data for Property Price and Occupancy Dynamics: Airbnb Bangkok Case Study

**Author:** Farand Diy Dat Mahazalfaa
**Institution:** Telkom University
**Subject**: Infrastructure and Big Data Technology

## Project Overview

This project implements a Big Data processing pipeline using Apache Spark (PySpark) to analyze the dynamics of the short-term rental market in Bangkok, Thailand. The study focuses on processing high-volume calendar and listing data to diagnose market health, specifically identifying indicators of structural oversupply and analyzing price volatility across fifty administrative districts for the projected period of 2025–2026.

The system is designed to handle dirty data ingestion, complex text parsing, and null-value imputation strategies within a distributed computing framework (simulated via Local Mode).

## Data Source

The dataset used in this research is obtained from **InsideAirbnb**, an independent, non-commercial set of tools and data that allows for the exploration of how Airbnb is being used in cities around the world.

* **Source URL:** http://insideairbnb.com/get-the-data/
* **Region:** Bangkok, Thailand
* **Snapshot Date:** 26 September, 2025
* **Required Files:**
    1.  `calendar.csv.gz`: Time-series data containing daily availability and pricing.
    2.  `listings.csv.gz`: Dimension data containing property attributes and descriptions.

*Note: Due to size constraints and licensing, raw data files are not included in this repository. Users must download the specific files from the link above and place them in the `data/` directory.*

## Technical Architecture

The project utilizes a Single-Node Spark Cluster architecture running on Windows Subsystem for Linux (WSL 2).

* **Processing Engine:** Apache Spark 3.5.0
* **Language:** Python 3.10 (PySpark Interface)
* **Storage Format:** Apache Parquet (Columnar Storage)
* **Visualization:** Matplotlib & Seaborn

### ETL Pipeline Logic
1.  **Ingestion:** Implements `multiLine=True` option to correctly parse listings with newline characters in description fields.
2.  **Transformation:**
    * **Currency Parsing:** Cleaning and casting string-formatted currency fields to double precision.
    * **Price Fallback Strategy:** Implements a `coalesce` logic to handle significant null values in the calendar dataset by substituting missing daily prices with the static base price from the listings dataset.
3.  **Aggregation:** Computes monthly occupancy rates and average pricing per district using Spark SQL optimization.

## Key Findings

1.  **Projected Market Dynamics (2025-2026):** ime-series comparison between average listing prices (Red) and occupancy rates (Blue). The chart reveals a significant gap between supply and demand, with city-wide occupancy averaging below 50%. The artificial peak in September suggests forward inventory blocking rather than genuine tourism demand.
   <img width="3600" height="1800" alt="viz_1_market_dynamics" src="https://github.com/user-attachments/assets/cda5483e-a2cf-4f1f-8ec2-7acdcee234d7" />

2.  **Premium District Segmentation:** Top 10 administrative districts by average base price. Central business districts like **Vadhana** and **Pathum Wan** command the highest premiums, driven by their proximity to luxury amenities, though they face intense competition from professional hosts.
<img width="3600" height="1800" alt="viz_2_premium_locations" src="https://github.com/user-attachments/assets/9af93d43-bb0e-4786-96ba-d2c52611ea33" />

3.  **Structural Oversupply Evidence:** Scatter plot correlation between Listing Price (X-axis) and Occupancy Rate (Y-axis). The dense clustering of data points in the lower quadrant (Occupancy < 40%) across all price ranges provides empirical evidence of **structural oversupply**, indicating that price reductions do not necessarily guarantee higher booking rates in a saturated market.
<img width="3600" height="1800" alt="viz_3_scatter_correlation" src="https://github.com/user-attachments/assets/7c750d8a-8913-4fc6-8810-6d17d048a40a" />


## Installation and Usage

### Prerequisites
* Python 3.10+
* Java Development Kit (JDK) 11
* Apache Spark 3.5.0
* WSL 2 (if running on Windows)

### Setup Instructions

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/Airbnb-Big-Data-Analytics-Spark.git](https://github.com/your-username/Airbnb-Big-Data-Analytics-Spark.git)
    cd Airbnb-Big-Data-Analytics-Spark
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Download Data:**
    Download `calendar.csv.gz` and `listings.csv.gz` from InsideAirbnb and place them in the `data/` folder.

4.  **Run ETL Pipeline:**
    Execute the Spark job to process raw data and generate Parquet output.
    ```bash
    python src/etl_pipeline_fixed.py
    ```

5.  **Run Analysis and Visualization:**
    Generate statistical reports and visual graphs.
    ```bash
    python src/visualize_data.py
    ```

## Project Structure

```text
├── data/                   # Raw data directory (excluded from version control)
├── output/                 # Processed Parquet files
├── src/
│   ├── etl_pipeline_fixed.py    # Main Spark ETL script
│   ├── visualize_data.py        # Visualization script (Pandas/Matplotlib)
│   ├── analysis_sanity_check.py # Data integrity validation script
│   └── ...
├── .gitignore              # Git configuration
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation

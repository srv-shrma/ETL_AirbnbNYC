# ETL_AirbnbNYC

ETL Process using MetaFlow Lib.

```
ETL_AirbnbNYC/
│
├── data/ # Directory for dataset
│ └── AB_NYC_2019.csv # dataset
│
├── scripts/ # Directory for scripts
│ └── ETL_AirbnbNYC.py # ETL script
│
├── README.md # Instructions and documentation
│
└── requirements.txt # Dependencies
```

Setup Instructions
Prerequisites
Python 3.8
PostgreSQL
Metaflow
Installation
1. Clone the repository:
```
git clone https://github.com/your-username/ETL_AirbnbNYC.git
cd ETL_AirbnbNYC
```

2. Install the required Python packages:
```
pip install -r requirements.txt
```

3. Set up PostgreSQL:
  - Create a PostgreSQL database and user.
  - Update the `ETL_AirbnbNYC.py` script with your PostgreSQL credentials.
  
4. Running the ETL Process
  - Place the Airbnb NYC dataset in the `data/` directory.
  - Run the ETL script:

```
python3 scripts/ETL_AirbnbNYC.py run
```

This will execute the ETL workflow, extracting data from the dataset, transforming it, and loading it into the PostgreSQL database.



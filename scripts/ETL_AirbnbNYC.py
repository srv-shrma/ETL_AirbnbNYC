from metaflow import FlowSpec, step, Parameter
import psycopg2
import csv

class AirbnbETLFlow(FlowSpec):

    # def __init__(self, start_from_step=None):
    #     super().__init__()
    #     self.start_from_step = start_from_step

    @step
    def start(self):
        self.data_file = '/Users/neha/Desktop/untitled folder/AB_NYC_2019.csv'
        self.db_name = 'airbnbnyc'
        self.db_user = 'postgres'
        self.db_password = 'admin'
        self.next(self.load_data)

    @step
    def load_data(self):
        conn = psycopg2.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password}")
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS places (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                host_id INT,
                host_name VARCHAR(255),
                neighbourhood_group VARCHAR(50),
                neighbourhood VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                room_type VARCHAR(50),
                price INT,
                minimum_nights INT,
                number_of_reviews INT,
                last_review DATE,
                reviews_per_month FLOAT,
                calculated_host_places_count INT,
                availability_365 INT
            );
        """)

        with open(self.data_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            for row in reader:
                # Handle empty or missing date fields
                if row[12] == "":
                    row[12] = None  # set to a default date or NULL 
                if row[9] == "":
                    row[9] = None  # set to a default numeric value or NULL 
                if row[13] == "":
                    row[13] = None    

                cur.execute(
                    "INSERT INTO places (id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, calculated_host_listings_count, availability_365) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    row
                )

        conn.commit()
        cur.close()
        conn.close()
        self.next(self.transform_data)

    @step
    def transform_data(self):
        conn = psycopg2.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password}")
        cur = conn.cursor()
        
        # Calculate average price per neighbourhood
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'transformed_places') THEN
                    CREATE TABLE transformed_places AS
                    SELECT neighbourhood, AVG(price) AS avg_price
                    FROM places
                    GROUP BY neighbourhood;
                END IF;
            END $$;
        """)

        # Normalize date and time columns
        cur.execute("""
            ALTER TABLE transformed_places
            ADD COLUMN review_date DATE,
            ADD COLUMN review_time TIME;
            
            UPDATE transformed_places tp
            SET review_date = COALESCE(p.last_review::date, CURRENT_DATE),
                review_time = COALESCE(TO_TIMESTAMP(p.last_review::text, 'YYYY-MM-DD')::time, CURRENT_TIME)
            FROM places p
            WHERE tp.neighbourhood = p.neighbourhood;


        """)

        # Handle missing values by filling with defaults
        cur.execute("""
            ALTER TABLE transformed_places
            ADD COLUMN reviews_per_month FLOAT DEFAULT 0.0,
            ADD COLUMN number_of_reviews INT DEFAULT 0;
        
            UPDATE transformed_places tp
            SET reviews_per_month = COALESCE(p.reviews_per_month, 0.0),
                number_of_reviews = COALESCE(p.number_of_reviews, 0)
            FROM places p
            WHERE tp.neighbourhood = p.neighbourhood;

        """)
        
        conn.commit()
        cur.close()
        conn.close()
        self.next(self.end)

    @step
    def end(self):
        print("ETL process completed successfully.")
        self.cleanup_database()

    def cleanup_database(self):
        try:
            conn = psycopg2.connect(f"dbname={self.db_name} user={self.db_user} password={self.db_password}")
            cur = conn.cursor()
            
            # Close database connection
            cur.close()
            conn.close()
            print("Database connection closed.")
        except Exception as e:
            print(f"Error closing database connection: {e}")

if __name__ == '__main__':
    
    AirbnbETLFlow()
    # flow = AirbnbETLFlow()

    # # start from transform_data step
    # flow.start_from_transform_data()

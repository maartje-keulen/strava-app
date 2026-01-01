import json
import psycopg2
from google.cloud import secretmanager
from google.oauth2 import service_account
import pandas as pd

def get_db_config():
    # Check if running on Streamlit Cloud (use service account from secrets)
    client = secretmanager.SecretManagerServiceClient()

    secret_name = "projects/strava-maartje/secrets/db-config/versions/latest"

    # Access the secret
    response = client.access_secret_version(name=secret_name)
    secret_data = response.payload.data.decode("UTF-8")

    # Parse the JSON configuration
    db_config = json.loads(secret_data)
    return db_config

def summarize_hours_from_db():
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Query to select data for 2024 and 2025 separately
        query = """
        SELECT 
            EXTRACT(YEAR FROM date) AS year, 
            sport_type AS sport, 
            SUM(elapsed_time) / 3600 AS hours
        FROM stravadata
        WHERE EXTRACT(YEAR FROM date) IN (2024, 2025)
        GROUP BY EXTRACT(YEAR FROM date), sport_type
        ORDER BY year, hours DESC;
        """

        # Execute the query
        cursor.execute(query)

        # Fetch the results
        rows = cursor.fetchall()

        # Convert the results to a DataFrame
        df = pd.DataFrame(rows, columns=["Year", "Sport", "Hours"])

        # Export the DataFrame to a CSV file
        df.to_csv("summary.csv", index=False)

        # Print confirmation
        print("Summary exported to summary.csv")

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")

    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

# Fetch and use the DB configuration
db_config = get_db_config()

if __name__ == "__main__":
    summarize_hours_from_db()
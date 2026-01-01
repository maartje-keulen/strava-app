import pandas as pd
import psycopg2
from psycopg2 import sql
import altair as alt
import plotly.express as px
import json
from google.cloud import secretmanager
from datetime import datetime
import streamlit as st

def get_db_config():
    # Check if running on Streamlit Cloud (use service account from secrets)
    if "gcp_service_account" in st.secrets:
        from google.oauth2 import service_account
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    else:
        # Local development (use application default credentials)
        client = secretmanager.SecretManagerServiceClient()

    secret_name = "projects/strava-maartje/secrets/db-config/versions/latest"

    # Access the secret
    response = client.access_secret_version(name=secret_name)
    secret_data = response.payload.data.decode("UTF-8")

    # Parse the JSON configuration
    db_config = json.loads(secret_data)
    return db_config

# Fetch and use the DB configuration
db_config = get_db_config()
#print(db_config)
#db_config = {
#    "user": st.secrets["user"],
#    "password": st.secrets["password"],
#    "host": st.secrets["host"],
#    "port": st.secrets["port"]
#}

try:
    # Connect to the database
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Define the table name
    table_name = "stravadata"

    # Query to select all data
    select_query = f"SELECT * FROM {table_name};"

    # Execute the query
    cursor.execute(select_query)

    # Fetch all rows
    rows = cursor.fetchall()

    # Fetch column names
    column_names = [desc[0] for desc in cursor.description]

    # Convert the result to a pandas DataFrame
    data_import = pd.DataFrame(rows, columns=column_names)

except psycopg2.Error as e:
    print(f"Error reading data: {e}")

finally:
    # Close the cursor and connection
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
        print("Database connection closed.")

data_import["hours"] = data_import["elapsed_time"] / 3600
data_import["year"] = data_import["year"].astype(str)
data_import["month"] = data_import["month"].astype(str)
data_import["month_name"] = data_import["date"].dt.strftime("%B")

#clean data
data_import["sport_type"] = data_import["sport_type"].replace({
    "VirtualRide": "Ride 🚲",
    "GravelRide": "Ride 🚲",
    "Ride": "Ride 🚲",
    "TrailRun": "Run 🏃‍♀️",
    "AlpineSki": "Snowboard",
    "Walk": "Walk the 🐶",
    "Workout": "Ultimate Frisbee 🥏",
    "WeightTraining": "Weight Training 💪",
    "RockClimbing": "Bouldering",
    "Yoga": "Stretching",
    "MountainBikeRide": "Ride 🚲", 
    "Hike": "Hike 🏔️",
    "Run": "Run 🏃‍♀️",
    "Crossfit": "Weight Training 💪",
})

#aggregate table

current_year = datetime.now().year
current_week = datetime.now().isocalendar()[1]

# Calculate avg_hours dynamically
def calculate_avg_hours(row):
    if int(row['year']) < current_year:
        return float(row['hours']) / 50
    elif int(row['year']) == current_year:
        # For the current year, divide by the number of weeks that have passed
        return float(row['hours']) / current_week
    else:
        # For future years (if applicable), return None or 0
        return None
    
old_data = pd.read_csv("data_before_2020.csv")
old_data["sport_type"] = old_data["sport_type"].replace({
    "Bike": "Ride 🚲",
    "Ultimate Frisbee": "Ultimate Frisbee 🥏",
    "Weight Training": "Weight Training 💪",
    "Hike": "Hike 🏔️",
    "Run": "Run 🏃‍♀️",
    "Football": "Football ⚽"
})
aggregated_df = data_import.groupby(["year", "sport_type"], as_index=False).agg(
    hours=("hours", "sum")).sort_values(by="hours", ascending=False)

aggregated_df['avg_hours'] = aggregated_df.apply(calculate_avg_hours, axis=1)
aggregated_df = pd.concat([aggregated_df, old_data], ignore_index=True)

top_sports = aggregated_df.groupby(["sport_type"], as_index=False).agg(hours=("hours", "sum"))
top_sports = top_sports.sort_values(by="hours", ascending=False)
top_sports["hours"] = top_sports["hours"].round(0).astype(int)
#st.bar_chart(data_import, x="year", y="hours", color="sport_type", horizontal=True)

#filter only most popular sports for yearly overview
filtered_sports = top_sports[top_sports['hours'] > 20]
filtered_aggregated_df = aggregated_df[aggregated_df['sport_type'].isin(filtered_sports['sport_type'])]

data_year_month = data_import.groupby(["year", "month", "sport_type"], as_index=False).agg(hours=("hours", "sum"))

#calculate metrics
active_days_per_year = data_import.groupby('year')['date'].nunique().reset_index()
print(active_days_per_year)
training_hours_per_year = data_import.groupby('year')['hours'].sum().reset_index()
print(training_hours_per_year)

target_sports = ["Run 🏃‍♀️", 
    "Ultimate Frisbee 🥏",
    "Weight Training 💪",
    "Ride 🚲"]
aggregated_df["sports_category"] = aggregated_df["sport_type"].apply(lambda x: x if x in target_sports else 'Other Sports')
sports_per_year = aggregated_df.groupby(['year', 'sports_category'])['hours'].sum().reset_index()

target_year = '2024'
compare_year = '2023'

#active days
active_2024 = active_days_per_year[active_days_per_year['year']==target_year]['date'].values[0]
active_2023 = active_days_per_year[active_days_per_year['year']==compare_year]['date'].values[0]
d_active_2024 = active_2024 - active_2023

#total training hours
training_2024 = training_hours_per_year[training_hours_per_year['year']==target_year]['hours'].values[0]
training_2023 = training_hours_per_year[training_hours_per_year['year']==compare_year]['hours'].values[0]
d_training_2024 = training_2024 - training_2023


#frisbee
frisbee_label = 'Ultimate Frisbee 🥏'
frisbee_target = sports_per_year[(sports_per_year['year']==target_year) & (sports_per_year['sports_category']=='Ultimate Frisbee 🥏')]['hours'].values[0]
frisbee_compare = sports_per_year[(sports_per_year['year']==compare_year) & (sports_per_year['sports_category']=='Ultimate Frisbee 🥏')]['hours'].values[0]

#ride
ride_label = 'Ride 🚲'
ride_target = sports_per_year[(sports_per_year['year']==target_year) & (sports_per_year['sports_category']=='Ride 🚲')]['hours'].values[0]
ride_compare = sports_per_year[(sports_per_year['year']==compare_year) & (sports_per_year['sports_category']=='Ride 🚲')]['hours'].values[0]

#run
run_label = 'Run 🏃‍♀️'
run_target = sports_per_year[(sports_per_year['year']==target_year) & (sports_per_year['sports_category']=='Run 🏃‍♀️')]['hours'].values[0]
run_compare = sports_per_year[(sports_per_year['year']==compare_year) & (sports_per_year['sports_category']=='Run 🏃‍♀️')]['hours'].values[0]

#weights
weights_label = 'Weight Training 💪'
weights_target = sports_per_year[(sports_per_year['year']==target_year) & (sports_per_year['sports_category']=='Weight Training 💪')]['hours'].values[0]
weights_compare = sports_per_year[(sports_per_year['year']==compare_year) & (sports_per_year['sports_category']=='Weight Training 💪')]['hours'].values[0]

#other
other_label = 'Other Sports'
other_target = sports_per_year[(sports_per_year['year']==target_year) & (sports_per_year['sports_category']=='Other Sports')]['hours'].values[0]
other_compare = sports_per_year[(sports_per_year['year']==compare_year) & (sports_per_year['sports_category']=='Other Sports')]['hours'].values[0]

def make_overview(dataset, input_x, input_y, input_color, input_title):
    overview = px.bar(dataset, x=input_x, #"sport_type"
    y=input_y, #"hours",
    color=input_color, #"year",
    barmode="stack",
    title=input_title,
    labels={"hours": "Total Hours", "sport_type": "Sport Type"},
    text="sport_type"  # Display values on the bars
)
    return overview


def make_bars(dataset, input_x, input_y, input_color, input_title):
    overview = px.bar(dataset, x=input_x, #"sport_type"
    y=input_y, #"hours",
    color=input_color, #"year",
    barmode="group",
    title=input_title,
    labels={"hours": "Total Hours", "sport_type": "Sport Type"},
    text="sport_type"  # Display values on the bars
        )
    return overview

def make_lines(dataset, input_x, input_y, input_color, input_title):
    overview = px.area(dataset, x=input_x,
    y=input_y,
    color=input_color,
    title=input_title,
    labels={"hours": "Average Hours per Week", "sport_type": "Sport Type"}
        )
    return overview

import plotly.graph_objects as go

# Sample data for the donut chart
labels = [frisbee_label, ride_label, run_label, weights_label, other_label]
values = [frisbee_target, ride_target, run_target, weights_target, other_target]

# Create the donut chart
fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.5)])

# Add custom text in the center of the donut
fig.update_layout(
    annotations=[
        {
            "text": "2024<br>900",
            "font_size": 20,
            "showarrow": False
        }
    ]
)



st.set_page_config(
    page_title="Training Efforts",
    layout="wide")

col = st.columns([1])[0]

with col:
    st.markdown('#### How my training focus changed over the years')
    st.markdown('How do I keep training consistently for over 15 years already? It is not just about discipline, perseverance and a strong inner drive. My shift in sports activities over time shows how I have been able to keep it up for so long. I try to be pragmatic and realistic and find ways to keep it fun. I am always looking for adjustments to adapt to my evolving lifestyle.')
    st.markdown('')

    graph2 = make_lines(filtered_aggregated_df, 'year', 'avg_hours', 'sport_type', 'Average Training Hours per Week per Sport')
    st.plotly_chart(graph2, use_container_width=True)

    st.divider()
    st.markdown('#### Every year looks different!')
    st.markdown('This graph shows how I adapted my training load every season to when it counts most for me to be fit and also to match my motivation level. Also for me it is impossible to stay motivated all the time. My favorite excuses: it is so cold and dark outside, I am too tired, I had an exhausting day at work… Enjoy what you do over executing perfectly!')
    st.markdown('')

    year_list = sorted(data_import.year.unique(), reverse=True)[::-1]
    selected_year = st.radio('Select a year:', year_list, index=len(year_list)-1, horizontal=True)
    df_selected_year = data_year_month[data_year_month.year == selected_year]
    df_selected_year_sorted = df_selected_year.sort_values(by="sport_type", ascending=False)

    graph3 = make_overview(df_selected_year_sorted, 'month', 'hours', 'sport_type', 'Training Hours per Month')
    st.plotly_chart(graph3, use_container_width=True)
    
    st.divider()
    st.markdown('#### 2024 was my most active year so far!')
    st.markdown('')

    subcol1, subcol2 = st.columns(2)
    
    with subcol1:
        st.metric(label="Active Days", value=int(active_2024), delta = int(d_active_2024))
    
    with subcol2:
        st.metric(label="Total Training Hours", value=int(training_2024), delta = int(d_training_2024))

    st.markdown('')
    st.markdown('#### Lets see what changed in 2024 compared to 2023')
    st.markdown('These are the total hours I have invested in every activity and how much that changed compared to 2023.')
    st.markdown('')
    # Display the chart in Streamlit    
    sub1, sub2, sub3, sub4, sub5 = st.columns(5)
    
    with sub1:
        st.metric(label=frisbee_label, value=int(frisbee_target), delta = int(frisbee_target-frisbee_compare))
    with sub2:
        st.metric(label=ride_label, value=int(ride_target), delta = int(ride_target-ride_compare))
    with sub3:
        st.metric(label=weights_label, value=int(weights_target), delta = int(weights_target-weights_compare))
    with sub4:
        st.metric(label=run_label, value=int(run_target), delta = int(run_target-run_compare))
    with sub5:
        st.metric(label=other_label, value=int(other_target), delta = int(other_target-other_compare))
    
    
    st.divider()
    st.markdown('#### All Time Favourite Sports since 2008')
    st.markdown('Total amount of hours I have spent on different sports in my adult life.')


    st.dataframe(top_sports,
                 column_order=("sport_type", "hours"),
                 hide_index=True,
                 width=None,
                 column_config={
                    "sport_type": st.column_config.TextColumn(
                        "Sports",
                    ),
                    "hours": st.column_config.ProgressColumn(
                        "Hours",
                        format="%f",
                        min_value=0,
                        max_value=max(top_sports.hours),
                     )}
                 )

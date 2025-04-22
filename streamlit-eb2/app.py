import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
import time
import numpy as np
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

# -----------------------
# Location Mapping
# -----------------------
LOCATION_MAPPING = {
    43: "Lower East Side",
    74: "East Harlem",
    132: "JFK Airport"
}

# -----------------------
# Metrics Calculation
# -----------------------
def calculate_metrics(actual_df, predicted_df):
    """Calculate MAE and MAPE between actual and predicted values"""
    # Convert datetime columns to datetime objects if they aren't already
    if 'pickup_hour' in actual_df.columns:
        actual_df['pickup_hour'] = pd.to_datetime(actual_df['pickup_hour'])
    if 'prediction_datetime' in predicted_df.columns:
        predicted_df['prediction_datetime'] = pd.to_datetime(predicted_df['prediction_datetime'])
    
    # Merge dataframes on datetime columns
    merged_df = pd.merge(
        actual_df,
        predicted_df,
        left_on='pickup_hour',
        right_on='prediction_datetime',
        how='inner'
    )
    
    # If no matching timestamps, return None
    if merged_df.empty:
        return None, None
    
    # Calculate MAE
    mae = np.mean(np.abs(merged_df['rides'] - merged_df['predicted_rides']))
    
    # Calculate MAPE (avoid division by zero)
    non_zero_mask = merged_df['rides'] > 0
    if non_zero_mask.sum() > 0:
        mape = np.mean(np.abs((merged_df['rides'][non_zero_mask] - merged_df['predicted_rides'][non_zero_mask]) / 
                             merged_df['rides'][non_zero_mask])) * 100
    else:
        mape = None
    
    return mae, mape

# -----------------------
# Athena Query Function
# -----------------------
@st.cache_data
def run_athena_query(hour, query: str, database: str, s3_output: str) -> pd.DataFrame:
    athena_client = boto3.client('athena', region_name="us-east-1")

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output}
    )

    query_execution_id = response['QueryExecutionId']
    state = 'RUNNING'

    while state in ['RUNNING', 'QUEUED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        if state in ['RUNNING', 'QUEUED']:
            time.sleep(1)

    if state != 'SUCCEEDED':
        reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise Exception(f"Athena query failed: {state} - {reason}")

    results = []
    columns = []
    next_token = None
    first_page = True

    while True:
        if next_token:
            result_set = athena_client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=next_token
            )
        else:
            result_set = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        if first_page:
            columns = [col['Label'] for col in result_set['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            first_page = False

        rows = result_set['ResultSet']['Rows']
        if next_token is None:
            rows = rows[1:]  # skip header

        for row in rows:
            results.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = result_set.get('NextToken')
        if not next_token:
            break

    df = pd.DataFrame(results, columns=columns)

    for col in df.columns:
        try:
            df[col] = pd.to_datetime(df[col])
        except:
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                pass

    return df

# -----------------------
# Streamlit App
# -----------------------
st.title("NYC Taxi Rides Forecast")

# Tabs
tab1, tab2 = st.tabs(["Athena", "RDS"])

# -----------------------
# Tab: Athena
# -----------------------
with tab1:
    # Pickup Location Input - changed to dropdown with three locations
    location_id = st.selectbox(
        "Select Pickup Location",
        options=list(LOCATION_MAPPING.keys()),
        format_func=lambda x: f"{LOCATION_MAPPING[x]} (ID: {x})",
        key="athena_location"
    )

    # Use Eastern Time (New York)
    eastern = ZoneInfo("America/New_York")
    now_ny = datetime.now(tz=eastern)

    # Calculate the same week last year in NY time
    end_date = now_ny - timedelta(days=358)
    start_date = now_ny - timedelta(days=365)

    # Round down to start of the hour
    start_rounded = start_date.replace(minute=0, second=0, microsecond=0)

    # Round up to next full hour if needed
    if end_date.minute > 0 or end_date.second > 0 or end_date.microsecond > 0:
        end_rounded = (end_date + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        end_rounded = end_date.replace(minute=0, second=0, microsecond=0)

    # Format for Athena query (still using naive-looking string)
    start_str = start_rounded.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_rounded.strftime("%Y-%m-%d %H:%M:%S")

    # Athena setup
    s3_output = 's3://jaath-buckets-0491f6b4-2be4-4ab9-9aa2-c62891ad4a9c/athena/'

    # Queries
    actual_query = f"""
    SELECT DISTINCT
        pickup_hour,
        rides
    FROM glue_transformed
    WHERE
        pickup_location_id = {location_id}
        AND pickup_hour BETWEEN '{start_str}' AND '{end_str}'
    ORDER BY pickup_hour;
    """

    predicted_query = f"""
    SELECT DISTINCT
        prediction_datetime,
        predicted_rides
    FROM test_predicted_values
    WHERE
        pickup_location_id = '{location_id}'
        AND prediction_datetime BETWEEN '{start_str}' AND '{end_str}'
    ORDER BY prediction_datetime;
    """

    # Run Queries
    try:
        actual_df = run_athena_query(now_ny.hour, actual_query, 'taxi_db_2324', s3_output)
        predicted_df = run_athena_query(now_ny.hour, predicted_query, 'predictions', s3_output)

        # Plot
        fig = px.line()
        if not actual_df.empty:
            fig.add_scatter(x=actual_df['pickup_hour'], y=actual_df['rides'], name='Actual Rides')
        if not predicted_df.empty:
            fig.add_scatter(x=predicted_df['prediction_datetime'], y=predicted_df['predicted_rides'], name='Predicted Rides')
        
        fig.update_layout(
            title=f"Taxi Rides Forecast for {LOCATION_MAPPING[location_id]} (ID: {location_id})",
            xaxis_title="Time",
            yaxis_title="Number of Rides"
        )

        st.plotly_chart(fig)
        
        # Calculate and display metrics
        if not actual_df.empty and not predicted_df.empty:
            mae, mape = calculate_metrics(actual_df, predicted_df)
            if mae is not None:
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Mean Absolute Error (MAE)", f"{mae:.2f}")
                with col2:
                    if mape is not None:
                        st.metric("Mean Absolute Percentage Error (MAPE)", f"{mape:.2f}%")
                    else:
                        st.metric("Mean Absolute Percentage Error (MAPE)", "N/A")
                st.info("Lower values indicate better prediction accuracy")

    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")

# -----------------------
# Tab: RDS
# -----------------------
with tab2:
    st.subheader("RDS: Actual vs Predicted Rides")

    # Pickup Location Input - changed to dropdown with three locations
    location_id_rds = st.selectbox(
        "Select Pickup Location",
        options=list(LOCATION_MAPPING.keys()),
        format_func=lambda x: f"{LOCATION_MAPPING[x]} (ID: {x})",
        key="rds_loc"
    )

    # RDS connection settings
    import sqlalchemy
    from sqlalchemy import create_engine, text

    rds_host = "rds-taxi.cst0aqomgoh3.us-east-1.rds.amazonaws.com"
    rds_db = "postgres"
    rds_user = "postgres"
    rds_password = "project#3password"
    rds_port = 5432

    # Create engine
    rds_engine = create_engine(
        f"postgresql+psycopg2://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db}"
    )

    # Use same time range
    try:
        with rds_engine.connect() as conn:
            actual_sql = text(f"""
            SELECT 
            TO_CHAR(pickup_hour::timestamp, 'YYYY-MM-DD HH24:MI:SS') as pickup_hour, 
            rides
            FROM taxi_rides
            WHERE pickup_location_id = :loc
            ORDER BY pickup_hour;
            """)
            predicted_sql = text(f"""
            SELECT 
            TO_CHAR(prediction_datetime::timestamp, 'YYYY-MM-DD HH24:MI:SS') as prediction_datetime, 
            predicted_rides
            FROM predicted_rides
            WHERE pickup_location_id = :loc
            ORDER BY prediction_datetime;
            """)

            actual_df_rds = pd.read_sql(actual_sql, conn, params={
            'loc': location_id_rds
            })

            predicted_df_rds = pd.read_sql(predicted_sql, conn, params={
            'loc': location_id_rds
            })

            # Convert string datetime columns to actual datetime objects
            if not actual_df_rds.empty:
                actual_df_rds['pickup_hour'] = pd.to_datetime(actual_df_rds['pickup_hour'])
            if not predicted_df_rds.empty:
                predicted_df_rds['prediction_datetime'] = pd.to_datetime(predicted_df_rds['prediction_datetime'])
    # try:
    #     with rds_engine.connect() as conn:
    #         actual_sql = text(f"""
    #             SELECT pickup_hour, rides
    #             FROM taxi_rides
    #             WHERE pickup_location_id = :loc
    #             ORDER BY pickup_hour;
    #         """)
    #         predicted_sql = text(f"""
    #             SELECT prediction_datetime, predicted_rides
    #             FROM predicted_rides
    #             WHERE pickup_location_id = :loc
    #             ORDER BY prediction_datetime;
    #         """)

    #         actual_df_rds = pd.read_sql(actual_sql, conn, params={
    #             'loc': location_id_rds
    #         })

    #         predicted_df_rds = pd.read_sql(predicted_sql, conn, params={
    #             'loc': location_id_rds
    #         })

            # Plot
            fig_rds = px.line()
            if not actual_df_rds.empty:
                fig_rds.add_scatter(x=actual_df_rds['pickup_hour'], y=actual_df_rds['rides'], name='Actual Rides')
            if not predicted_df_rds.empty:
                fig_rds.add_scatter(x=predicted_df_rds['prediction_datetime'], y=predicted_df_rds['predicted_rides'], name='Predicted Rides')
            
            fig_rds.update_layout(
                title=f"RDS - Taxi Rides Forecast for {LOCATION_MAPPING[location_id_rds]} (ID: {location_id_rds})",
                xaxis_title="Time",
                yaxis_title="Number of Rides"
            )
            st.plotly_chart(fig_rds)
            
            # Calculate and display metrics
            if not actual_df_rds.empty and not predicted_df_rds.empty:
                mae, mape = calculate_metrics(actual_df_rds, predicted_df_rds)
                if mae is not None:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Mean Absolute Error (MAE)", f"{mae:.2f}")
                    with col2:
                        if mape is not None:
                            st.metric("Mean Absolute Percentage Error (MAPE)", f"{mape:.2f}%")
                        else:
                            st.metric("Mean Absolute Percentage Error (MAPE)", "N/A")
                    st.info("Lower values indicate better prediction accuracy")

    except Exception as e:
        st.error(f"❌ Error connecting to RDS: {e}")

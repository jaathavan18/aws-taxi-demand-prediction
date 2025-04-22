# AWS-Based Real-Time Taxi Demand Prediction System 🚖📊

A scalable, serverless data lake and machine learning pipeline for forecasting NYC Yellow Taxi ride demand using AWS services. This system ingests, transforms, models, and visualizes taxi demand predictions in real-time, delivering actionable insights via interactive dashboards.

---

## 📌 Project Overview

This project builds an end-to-end taxi demand forecasting platform leveraging AWS cloud-native services and ML models. It processes trip data from 2023–2024, forecasts future demand, and delivers insights through dashboards deployed on Elastic Beanstalk.

---

## 📊 Tech Stack

- **AWS Lambda** — Data ingestion & orchestration  
- **AWS Glue** — Data processing & ETL  
- **Amazon S3** — Centralized data storage (data lake)  
- **AWS Athena** — Serverless data querying  
- **Amazon RDS (PostgreSQL)** — Structured storage for transformed and prediction data  
- **Elastic Beanstalk** — Streamlit dashboard deployment  
- **LightGBM** — ML modeling (lag features, FFT, Prophet forecasts)  
- **Streamlit** — Real-time visualization dashboards  

---

## 📈 Pipeline Architecture  

1. **Data Ingestion**  
   - Download raw taxi data via Lambda
   - Store data in an S3 data lake (structured by year/month)

2. **Data Processing**  
   - Trigger AWS Glue jobs for data filtering & transformation
   - Catalog data with Glue Crawlers

3. **ML Modeling**  
   - Query data with Athena for training
   - Train LightGBM models with lag, FFT, and Prophet features
   - Generate demand predictions for 2024

4. **Data Storage**  
   - Store transformed & predicted data in both S3 and PostgreSQL (RDS)

5. **Visualization & Monitoring**  
   - Deploy a Streamlit dashboard on Elastic Beanstalk  
   - Display MAE, MAPE, and demand forecasts for selected locations  
   - Use caching with hourly invalidation  

---

## 📂 Project Structure

```
├── lambda/
│   ├── raw_data_glue.py
├── glue_jobs/
│   ├── iadFilterAndTransform.py
├── models/
│   ├── s3 bucket/
├── streamlit_app/
│   └── dashboard/
├── sql_queries/
│   ├── athena/
│   ├── rds/
├── README.md
└── requirements.txt
```

---

## 🚀 Deployment

- Deploy Lambda functions via AWS Console or AWS CLI  
- Schedule Glue jobs with triggers  
- Set up Athena queries and views  
- Host Streamlit dashboard on Elastic Beanstalk  

---

## 📊 Dashboard Features

- Real-time demand forecasts  
- Metrics: MAE, MAPE per location  
- Location selection dropdown  
- Model comparison (Lag vs FFT vs Prophet-based models)

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

# 🎮 Game Pricing Intelligence

An end-to-end analytics pipeline that analyzes Steam game pricing, review patterns, and market segmentation using Python-based data processing.

This project answers structured business questions by transforming raw marketplace data into analytics-ready data marts. It simulates how analytics teams convert raw data into structured, decision-ready outputs.

---

## 📌 Project Purpose

The goal of this project is to replicate a real-world analytics workflow:

- Ingest raw marketplace data
- Clean and normalize fields
- Engineer analytical features
- Build aggregated data marts
- Answer business-driven pricing questions

This project focuses on pricing strategy, review behavior, and genre-based performance in the video game market.

---

## 🧠 Business Questions

### Q1: Pricing Trends  
How do average prices vary by season and over time?

### Q2: Genre & Review Performance  
How do different genres compare in:
- Median price  
- Average review count  
- Overall market positioning  

### Q3: Price Segmentation  
How do pricing tiers affect:
- Ownership ranges  
- Review density  
- Market segmentation  

---

## 🏗️ Project Structure

game-pricing-intelligence/
│
├── backend/
│ └── app/
│ ├── analytics/
│ │ ├── build_q1_marts.py
│ │ ├── build_q2_marts.py
│ │ └── build_q3_marts.py
│ │
│ └── ingestion/
│ └── clean_games.py
│
├── data/
│ ├── games.csv
│ ├── games_clean.csv
│ └── marts/
│
├── business_questions.md
├── requirements.txt
└── README.md


---

## ⚙️ How It Works

### 1️⃣ Data Ingestion & Cleaning  
File: `clean_games.py`

- Removes invalid or incomplete records  
- Normalizes pricing formats  
- Standardizes review counts  
- Cleans ownership ranges  
- Prepares dataset for analytical processing  

**Output:** `games_clean.csv`

---

### 2️⃣ Analytics Mart Generation  

Each `build_qX_marts.py` script:

- Aggregates engineered features  
- Computes grouped statistics  
- Performs segmentation analysis  
- Writes analytics-ready CSV outputs  
- Stores results in `data/marts/`  

Examples of generated outputs:
- Pricing by season
- Genre-level review summaries
- Price segmentation summaries
- Feature-level aggregated metrics

These marts simulate production-ready analytical tables used for dashboards and reporting.

---

## 🛠 Tech Stack

- Python
- Pandas
- NumPy
- Modular script-based architecture
- CSV-based data storage

---

## 📊 Current Phase

✅ Data cleaning pipeline  
✅ Feature engineering  
✅ Analytics mart generation  
🔜 Database integration  
🔜 API layer  
🔜 Dashboard integration  
🔜 Automated scheduling  

---

## 🚀 How to Run

### 1. Install dependencies
pip install -r requirements.txt

### 2. Run data cleaning
python backend/app/ingestion/clean_games.py

### 3. Build analytics marts
python backend/app/analytics/build_q1_marts.py
python backend/app/analytics/build_q2_marts.py
python backend/app/analytics/build_q3_marts.py
Generated outputs will appear in:

## 📈 Why This Project Matters
This project demonstrates:

- Structured analytics thinking  
- Feature engineering  
- Aggregation logic design  
- Business-question-driven modeling  
- Modular data pipeline architecture  

It reflects how analytics and data engineering teams transform raw marketplace data into decision-ready outputs.

## 👤 Author

Harrison Wier  
Computer Science – Data Science Concentration  
Florida Gulf Coast University  
Expected Graduation: 2027  
import sqlite3
import pandas as pd
import google.generativeai as genai
from fastapi import FastAPI
from models import DQReport
import json

app = FastAPI()

# Configure Gemini
genai.configure(
    api_key="AQ.Ab8RN6IuYnoH535sNV6oao9RMBhGvcBIUecIalfrPznNq0I9YA")
model = genai.GenerativeModel('gemini-pro')


def get_ai_rules(df_sample):
    prompt = f"Analyze these columns: {list(df_sample.columns)}. Generate DQ rules in JSON for NotNull, Range (0 or 100), and Domain (Yes/No). Return ONLY a JSON list: [{{'column': 'COL', 'logic': 'NotNull/Range/Domain', 'message': 'msg'}}]"
    response = model.generate_content(prompt)
    return json.loads(response.text.replace("```json", "").replace("```", "").strip())


@app.post("/run-dq")
async def run_data_quality():
    conn = sqlite3.connect('local_dq.db')
    df = pd.read_sql("SELECT * FROM supplier_data", conn)
    conn.close()

    rules = get_ai_rules(df.head(1))
    report = []

    for rule in rules:
        col = rule['column']
        logic = rule['logic']

        # Manual execution replacing Scala logic[cite: 1]
        if logic == "NotNull":
            failed = df[df[col].isna()]
        elif logic == "Range":
            failed = df[~df[col].isin([0, 100])]
        elif logic == "Domain":
            valid = ['Yes', 'No'] if 'FLG' in col else ['Y', 'N']
            failed = df[~df[col].isin(valid)]
        else:
            continue

        report.append(DQReport(
            column=col, status="FAILED" if not failed.empty else "PASSED",
            count=len(failed), samples=failed.to_json(orient="records") if not failed.empty else None
        ))
    return report

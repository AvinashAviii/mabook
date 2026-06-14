from fastapi import FastAPI, Path, HTTPException, Query
from pydantic import BaseModel
import json

app = FastAPI()  # object of FastAPI CLASS

# helper function to load data


def load_data():
    with open("patients.json", 'r') as f:
        data = json.load(f)
    return data

# decorator ,with the help of decorator a route is created, endpoint which will route to "/"


@app.get("/")
def hello():  # function /method
    return {"message": "PATIENT MANAGEMENT SYSTEM API"}


@app.get("/about")
def about():
    return {"message": "This is a simple API for managing patients in a hospital."}


@app.get("/view")
def view_patient():
    data = load_data()

    return data


@app.get("/patient/{patient_id}")
def view_patient(patient_id: str = Path(..., description='Id of Patient', example='P001')):
    data = load_data()

    if patient_id in data:
        return data[patient_id]
    raise HTTPException(status_code=404, detail='Patient Not Found')


@app.get('/sort')
def sort_patient(sort_by: str = Query(..., description='Sort on the basis of Height,Weight and BMI'), order: str = Query('asc', description='sort in asc or desc order')):
    valid_fields = ["height", "weight", "bmi"]

    if sort_by not in valid_fields:
        raise HTTPException(
            status_code=400, detail=f'invalid filed select from {valid_fields}')
    if order not in ["asc", "desc"]:
        raise HTTPException(status_code=400, detail='select valid order')

    data = load_data()
    sort_order = True if order == 'desc' else False
    sorted_data = sorted(data.values(), key=lambda x: x.get(
        sort_by, 0), reverse=sort_order)
    return sorted_data

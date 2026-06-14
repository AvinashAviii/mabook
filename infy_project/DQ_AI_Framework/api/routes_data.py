import os
import uuid
import shutil
import logging
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Query
import pandas as pd

from core.data_loader import data_loader
from models.schemas import UploadResponse
from config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data", tags=["Data Management"])


@router.post("/upload", response_model=UploadResponse)
async def upload_dataset(
    file: UploadFile = File(...),
    dataset_id: Optional[str] = Query(None, description="Custom dataset ID"),
):
    """
    Upload a CSV file for data quality analysis.

    - Ingests the file into PySpark
    - Generates data profile
    - Returns schema, stats, and sample data
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(400, "Only CSV files are supported")

    # Save uploaded file
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    file_id = dataset_id or str(uuid.uuid4())[:8]
    file_path = os.path.join(settings.UPLOAD_DIR, f"{file_id}_{file.filename}")

    try:
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # Load into PySpark
        did, metadata = data_loader.load_csv(file_path, dataset_id=file_id)

        return UploadResponse(
            dataset_id=did,
            filename=file.filename,
            row_count=metadata["row_count"],
            column_count=metadata["column_count"],
            columns=metadata["columns"],
            dtypes=metadata["dtypes"],
            sample_data=metadata["sample_data"],
        )

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        # Cleanup on failure
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(500, f"Failed to process file: {str(e)}")


@router.get("/datasets")
async def list_datasets():
    """List all loaded datasets"""
    return {"datasets": data_loader.list_datasets()}


@router.get("/datasets/{dataset_id}")
async def get_dataset_info(dataset_id: str):
    """Get detailed info about a specific dataset"""
    try:
        metadata = data_loader.get_metadata(dataset_id)
        return metadata
    except KeyError:
        raise HTTPException(404, f"Dataset '{dataset_id}' not found")


@router.get("/datasets/{dataset_id}/sample")
async def get_dataset_sample(dataset_id: str, n: int = Query(10, ge=1, le=1000)):
    """Get sample rows from a dataset"""
    try:
        sample = data_loader.get_sample(dataset_id, n)
        return {"dataset_id": dataset_id, "sample_size": len(sample), "data": sample}
    except KeyError:
        raise HTTPException(404, f"Dataset '{dataset_id}' not found")


@router.post("/upload/demo")
async def upload_demo_dataset():
    """
    Upload a demo employee dataset for testing.
    Includes intentional data quality issues.
    """
    demo_data = {
        "employee_id": ["E001", "E002", "E003", "E004", "E005",
                        "E006", "E007", None, "E009", "E003"],  # null + duplicate
        "first_name": ["John", "Jane", "Bob", "", "Alice",
                       "Charlie", "Diana", "Edward", "Fiona", "Bob"],
        "last_name": ["Doe", "Smith", "Johnson", "Williams", "Brown",
                      "Davis", "Miller", "Wilson", "Moore", "Johnson"],
        "email": ["john.doe@company.com", "jane.smith@company.com",
                  "bob.j@company.com", "invalid-email", "alice.b@company.com",
                  "charlie.d@company.com", None, "edward.w@company.com",
                  "fiona.m@company.com", "bob.j@company.com"],
        # 200 and -5 are invalid
        "age": [30, 25, 45, 200, 35, 28, 42, -5, 31, 45],
        "department": ["Engineering", "Marketing", "Engineering", "HR", "Engineering",
                       "Sales", "Marketing", "InvalidDept", "Engineering", "Engineering"],
        "salary": [85000, 72000, 95000, 65000, 88000,
                   70000, 78000, None, 92000, 95000],
        "hire_date": ["2020-01-15", "2019-06-20", "2018-03-10", "2021-11-01",
                      "2020-07-22", "2022-01-05", "2017-09-15", "2023-02-28",
                      "2021-04-12", "2018-03-10"],
        "status": ["active", "active", "active", "inactive", "active",
                   "active", "active", "active", "probation", "active"],
        "phone": ["555-0101", "555-0102", "555-0103", "12345", "555-0105",
                  "555-0106", "555-0107", "555-0108", "555-0109", "555-0103"],
        "role": ["Engineer", "Manager", "Senior Engineer", "Analyst", "Engineer",
                 "Sales Rep", "Designer", "Engineer", "Engineer", "Senior Engineer"],
    }

    pdf = pd.DataFrame(demo_data)
    did, metadata = data_loader.load_from_pandas(
        pdf, dataset_id="demo_employees")

    return UploadResponse(
        dataset_id=did,
        filename="demo_employees.csv",
        row_count=metadata["row_count"],
        column_count=metadata["column_count"],
        columns=metadata["columns"],
        dtypes=metadata["dtypes"],
        sample_data=metadata["sample_data"],
    )

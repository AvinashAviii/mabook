from api.routes_rules import router as rules_router
from api.routes_reports import router as reports_router
from api.routes_validation import router as validation_router
from api.routes_analysis import router as analysis_router
from api.routes_data import router as data_router
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from core.spark_manager import spark_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown"""
    # Startup
    logger.info(f"🚀 Starting {settings.APP_NAME} v{settings.APP_VERSION}")

    # Create directories
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    os.makedirs(settings.REPORT_DIR, exist_ok=True)

    # Initialize Spark
    spark = spark_manager.get_or_create_session()
    logger.info(f"⚡ Spark {spark.version} initialized")

    # Check Gemini
    from ai.gemini_client import gemini_client
    if gemini_client.is_available:
        logger.info("🤖 Gemini AI client ready")
    else:
        logger.warning("🤖 Gemini AI not configured (set GEMINI_API_KEY)")

    yield

    # Shutdown
    logger.info("🛑 Shutting down...")
    spark_manager.stop()


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    ## AI-Powered Data Quality Framework
    
    A production-grade DQ framework that uses **Gemini AI** to automatically 
    suggest data quality rules and **PySpark** to execute them at scale.
    
    ### Features
    - 📤 Upload CSV datasets
    - 🤖 AI-powered rule suggestion (NotNull, Range, Domain, Regex, Conditional)
    - ⚡ PySpark-based rule execution
    - 📊 Structured validation reports
    - 🔍 Anomaly detection and audit trails
    - 🔔 Async alerting (Email, SNS)
    """,
    lifespan=lifespan,
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers

app.include_router(data_router)
app.include_router(analysis_router)
app.include_router(validation_router)
app.include_router(reports_router)
app.include_router(rules_router)


@app.get("/", tags=["Health"])
async def root():
    return {
        "app": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "status": "running",
        "spark_active": spark_manager._spark is not None,
    }


@app.get("/health", tags=["Health"])
async def health_check():
    from ai.gemini_client import gemini_client
    return {
        "status": "healthy",
        "spark": {
            "active": spark_manager._spark is not None,
            "version": spark_manager.spark.version if spark_manager._spark else None,
        },
        "gemini": {
            "available": gemini_client.is_available,
            "model": settings.GEMINI_MODEL,
        },
        "config": {
            "pass_threshold": settings.DQ_PASS_THRESHOLD,
            "anomaly_sample_size": settings.ANOMALY_SAMPLE_SIZE,
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        workers=1,  # PySpark needs single process
    )

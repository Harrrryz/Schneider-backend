from typing import Dict, Any
from contextlib import asynccontextmanager
import sqlite3
import ormar
import databases
import sqlalchemy
from enum import Enum
from datetime import datetime
from typing import Optional, List, Sequence, TypeVar, Generic
from fastapi import FastAPI, HTTPException, Depends, Query, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import pandas as pd

from backend.utils import add_Batch_Size_impact_column, add_Consumption_class_column, add_Consumption_value_column, add_Coverage_class_column, add_Pending_Order_value_column, add_Safety_Stock_impact_column, add_benefit_column, add_days_column, add_days_objective_column, add_stock_class_column, add_stock_value_column


DB_NAME = "src/backend/data/SchneiderDatabase.db"
sqlite_url = f"sqlite:///{DB_NAME}"
engine = sqlalchemy.create_engine(sqlite_url)  # type: ignore
base_ormar_config = ormar.OrmarConfig(
    metadata=sqlalchemy.MetaData(),  # type: ignore
    database=databases.Database(sqlite_url),
    engine=engine,  # type: ignore
)

T = TypeVar('T')


# ORM Models
class InventoryModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="my_table")

    Part_Number: str = ormar.String(
        primary_key=True, max_length=255, name="Part Number")  # type: ignore
    Description: str = ormar.String(
        max_length=255, nullable=True)  # type: ignore
    Part_sub_commodity: str = ormar.String(
        max_length=255, nullable=True, name="Part sub commodity")  # type: ignore
    PUR_Grp: int = ormar.Integer(nullable=True, name="PUR Grp")  # type: ignore
    Mini_Business: str = ormar.String(
        max_length=255, nullable=True, name="Mini Business")  # type: ignore
    Product: str = ormar.String(max_length=255, nullable=True)  # type: ignore
    Order_specific_part: str = ormar.String(
        max_length=255, nullable=True, name="Order specific part")  # type: ignore
    Procurement_mode: str = ormar.String(
        max_length=255, nullable=True, name="Procurement mode")  # type: ignore
    Location: str = ormar.String(max_length=255, nullable=True)  # type: ignore
    Stock_qty: float = ormar.Float(
        nullable=True, name="Stock qty")  # type: ignore
    Pending_orders: int = ormar.Integer(
        nullable=True, name="Pending orders")  # type: ignore
    Part_standard_price: float = ormar.Float(
        nullable=True, name="Part standard price")  # type: ignore
    Safety_Stock: int = ormar.Integer(
        nullable=True, name="Safety Stock")  # type: ignore
    Batch_size: int = ormar.Integer(
        nullable=True, name="Batch size")  # type: ignore
    Yearly_consumption: float = ormar.Float(
        nullable=True, name="Yearly consumption")  # type: ignore
    Batch_size_impact: float = ormar.Float(
        nullable=True, name="Batch size impact")  # type: ignore
    Stock_Value: float = ormar.Float(
        nullable=True, name="Stock Value")  # type: ignore
    Days: float = ormar.Float(nullable=True)  # type: ignore
    Safety_Stock_impact: float = ormar.Float(
        nullable=True, name="Safety Stock impact")  # type: ignore
    Pending_Order_value: float = ormar.Float(
        nullable=True, name="Pending Order value")  # type: ignore
    Consumption_value: float = ormar.Float(
        nullable=True, name="Consumption value")  # type: ignore
    Coverage_class: str = ormar.String(
        max_length=255, nullable=True, name="Coverage class")  # type: ignore
    Consumption_class: str = ormar.String(
        nullable=True, name="Consumption class", max_length=300)  # type: ignore
    Stock_Class: str = ormar.String(
        max_length=255, nullable=True, name="Stock Class")  # type: ignore
    Days_Objective: float = ormar.Float(
        nullable=True, name="Days Objective")  # type: ignore
    Benefit: float = ormar.Float(nullable=True)  # type: ignore

    # Add more fields as needed

# Pydantic Models for API


class InventoryItem(BaseModel):  # For API responses.
    Part_Number: str
    Description: Optional[str]
    Part_sub_commodity: Optional[str]
    PUR_Grp: Optional[int]
    Mini_Business: Optional[str]
    Product: Optional[str]
    Order_specific_part: Optional[str]
    Procurement_mode: Optional[str]
    Location: Optional[str]
    Stock_qty: Optional[float]
    Pending_orders: Optional[int]
    Part_standard_price: Optional[float]
    Safety_Stock: Optional[int]
    Batch_size: Optional[int]
    Yearly_consumption: Optional[float]
    Batch_size_impact: Optional[float]
    Stock_Value: Optional[float]
    Days: Optional[float]
    Safety_Stock_impact: Optional[float]
    Pending_Order_value: Optional[float]
    Consumption_value: Optional[float]
    Coverage_class: Optional[str]
    Consumption_class: Optional[str]
    Stock_Class: Optional[str]
    Days_Objective: Optional[float]
    Benefit: Optional[float]
    # Add all fields here


class ProcurementBarChartModel(BaseModel):
    Order_specific_sum: float
    kanban_sum: float
    MRP_sum: float


class CoverageClassBarChartModel(BaseModel):
    Order_specific_sum: float
    fifteen_Days_sum: float
    thirty_Days_sum: float
    ninety_Days_sum: float
    ninety_plus_Days_sum: float


class PaginateModel(BaseModel, Generic[T]):
    page: int
    per_page: int
    total_items: int
    items: Sequence[T]


async def pagniate_inventory(page: int, per_page: int) -> PaginateModel[InventoryModel]:
    total_items = await InventoryModel.objects.count()
    inventory = await InventoryModel.objects.limit(per_page).offset((page-1)*per_page).all()
    return PaginateModel(
        page=page,
        per_page=per_page,
        total_items=total_items,
        items=inventory
    )

# Utility Functions


async def init_db_and_tables():
    base_ormar_config.metadata.create_all(engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/get_inventory/", response_model=PaginateModel[InventoryItem])
async def read_inventory(
    page: int,
    per_page: int,
    Part_sub_commodity: str = "",
    Product: str = "",
    location: str = "",
) -> PaginateModel[InventoryModel]:
    try:
        query = InventoryModel.objects
        if Part_sub_commodity:
            query = query.filter(Part_sub_commodity=Part_sub_commodity)
        if Product:
            query = query.filter(Product=Product)
        if location:
            query = query.filter(Location=location)

        total_items = await query.count()
        inventoryItems = await query.offset((page - 1) * per_page).limit(per_page).all()
        return PaginateModel[InventoryModel](
            page=page,
            per_page=per_page,
            total_items=total_items,
            items=inventoryItems)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/Provutement_mode_bar_chart_data", response_model=ProcurementBarChartModel)
async def get_procurement_mode_summary() -> ProcurementBarChartModel:
    Order_specific_part_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'Order specific'
    AND "Stock Value" IS NOT NULL;
    """
    kanban_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'kanban'
    AND "Stock Value" IS NOT NULL;
    """
    MRP_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'MRP'
    AND "Stock Value" IS NOT NULL;
    """
    Order_specific_part_result = await base_ormar_config.database.fetch_one(Order_specific_part_query)
    print(Order_specific_part_result)
    kanban_query_result = await base_ormar_config.database.fetch_one(kanban_query)
    print(kanban_query_result)
    MRP_query_result = await base_ormar_config.database.fetch_one(MRP_query)
    print(MRP_query_result)

    return ProcurementBarChartModel(Order_specific_sum=Order_specific_part_result["Total_Stock_Value"] if Order_specific_part_result else 0, kanban_sum=kanban_query_result["Total_Stock_Value"] if kanban_query_result else 0, MRP_sum=MRP_query_result["Total_Stock_Value"] if MRP_query_result else 0)


@app.get("/coverage_class_bar_chart_data", response_model=CoverageClassBarChartModel)
async def get_coverage_class_summary() -> CoverageClassBarChartModel:
    Order_specific_part_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = 'Order specific'
    AND "Stock Value" IS NOT NULL;
    """
    fifteen_Days_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<15 days'
    AND "Stock Value" IS NOT NULL;
    """
    thirty_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<30 days'
    AND "Stock Value" IS NOT NULL;
    """
    ninety_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<90 days'
    AND "Stock Value" IS NOT NULL;
    """
    ninety_plus_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '>90 days'
    AND "Stock Value" IS NOT NULL;
    """
    Order_specific_part_result = await base_ormar_config.database.fetch_one(Order_specific_part_query)
    print(Order_specific_part_result)
    fifteen_Days_result = await base_ormar_config.database.fetch_one(fifteen_Days_query)
    print(fifteen_Days_result)
    thirty_Days_result = await base_ormar_config.database.fetch_one(thirty_Days_query)
    ninety_Days_result = await base_ormar_config.database.fetch_one(ninety_Days_query)
    ninety_plus_Days_result = await base_ormar_config.database.fetch_one(ninety_plus_Days_query)

    return CoverageClassBarChartModel(Order_specific_sum=Order_specific_part_result["Total_Stock_Value"] if Order_specific_part_result else 0, fifteen_Days_sum=fifteen_Days_result["Total_Stock_Value"] if fifteen_Days_result else 0, thirty_Days_sum=thirty_Days_result["Total_Stock_Value"] if thirty_Days_result else 0, ninety_Days_sum=ninety_Days_result["Total_Stock_Value"] if ninety_Days_result else 0, ninety_plus_Days_sum=ninety_plus_Days_result["Total_Stock_Value"] if ninety_plus_Days_result else 0)


@app.post("/inventory/upload")
async def upload_inventory(file: UploadFile = File(...)):

    if file.filename is None or not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400, detail="Invalid file format. Only Excel files are supported")

    excel_file = file.file
    table_name = "my_table"

    try:
        df = pd.read_excel(excel_file)

    except FileNotFoundError:
        print(f"Error: Excel file '{excel_file}' not found.")
        exit()

    conn = sqlite3.connect(DB_NAME)

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

    # Needs to be copy-pasted from ipynb file
    add_stock_value_column(DB_NAME, table_name)
    add_days_column(DB_NAME, table_name)
    add_Safety_Stock_impact_column(DB_NAME, table_name)
    add_Batch_Size_impact_column(DB_NAME, table_name)
    add_Pending_Order_value_column(DB_NAME, table_name)
    add_Consumption_value_column(DB_NAME, table_name)
    add_Coverage_class_column(DB_NAME, table_name)
    add_Consumption_class_column(DB_NAME, table_name)
    add_stock_class_column(DB_NAME, table_name)
    add_days_objective_column(DB_NAME, table_name)
    add_benefit_column(DB_NAME, table_name)

    return {"message": "File uploaded and processed successfully"}

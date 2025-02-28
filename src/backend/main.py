from typing import Annotated, Dict, Any, Union
from contextlib import asynccontextmanager
import sqlite3
import jwt
from jwt.exceptions import InvalidTokenError
import ormar
import databases
import sqlalchemy
from enum import Enum
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Sequence, TypeVar, Generic
from fastapi import FastAPI, HTTPException, Depends, Query, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import pandas as pd
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi import Depends, FastAPI, HTTPException, status
from passlib.context import CryptContext
from backend.utils import add_Batch_Size_impact_column, add_Consumption_class_column, add_Consumption_value_column, add_Coverage_class_column, add_Pending_Order_value_column, add_Safety_Stock_impact_column, add_benefit_column, add_days_column, add_days_objective_column, add_stock_class_column, add_stock_value_column

# Constants
SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

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
class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class UserModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")
    id: int = ormar.Integer(primary_key=True)  # type: ignore
    userName: str = ormar.String(max_length=255,)  # type: ignore
    password: str = ormar.String(max_length=255)  # type: ignore


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
    user: UserModel = ormar.ForeignKey(
        UserModel, related_name='inventory_list')

    # Add more fields as needed

# Pydantic Models for API


class InventoryItemDto(BaseModel):  # For API responses.
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


class ProcurementBarChartModelDto(BaseModel):
    user_id: int
    Order_specific_sum: float
    kanban_sum: float
    MRP_sum: float


class CoverageClassBarChartModelDto(BaseModel):
    user_id: int
    Order_specific_sum: float
    fifteen_Days_sum: float
    thirty_Days_sum: float
    ninety_Days_sum: float
    ninety_plus_Days_sum: float


class UserDto(BaseModel):
    userName: str
    password: str


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


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
# Utility Functions


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def fake_decode_token(token):
    return UserModel(
        username=token + "fakedecoded", email="john@example.com", full_name="John Doe"
    )


async def authenticate_user(username: str, password: str):
    user = await UserModel.objects.get_or_none(userName=username)
    if not user:
        return False
    if not verify_password(password, user.password):
        return False
    return user


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        print("payload sub: " + username)
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except InvalidTokenError:
        raise credentials_exception
    user = await UserModel.objects.get_or_none(userName=token_data.username)
    if user is None:
        raise credentials_exception
    return user


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def init_db_and_tables():
    base_ormar_config.metadata.drop_all(engine)
    base_ormar_config.metadata.create_all(engine)
    init_users = ['Harry', 'Leo']
    for user_name in init_users:
        pwd = get_password_hash('12345678')
        existing_user = await UserModel.objects.get_or_none(userName=user_name)
        if existing_user:
            raise HTTPException(status_code=400, detail="User already exists!")
        user = UserModel(userName=user_name, password=pwd)
        await user.save()

    excel_file = "src/backend/data/data.xlsx"
    table_name = "my_table"

    try:
        df = pd.read_excel(excel_file)

    except FileNotFoundError:
        print(f"Error: Excel file '{excel_file}' not found.")
        exit()

    conn = sqlite3.connect(DB_NAME)

    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

    # print very object in Usermodel
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
    # for each row in my_table, create a user attribute
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `user` INTEGER")
    update_query = f"""
        UPDATE `{table_name}`
        SET `user` = 1
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()


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


@app.post('/token')
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    # Replace with your actual user validation logic
    user = await authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="User does not exist!")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.userName, "id": user.id}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


@app.post("/create_user/", tags=['user'], response_model=UserModel)
async def create_user(userDto: UserDto) -> UserModel:
    existing_user = await UserModel.objects.get_or_none(userName=userDto.userName)
    if existing_user:
        raise HTTPException(status_code=400, detail="User already exists!")
    pwd = get_password_hash(userDto.password)
    user = UserModel(userName=userDto.userName, password=pwd)
    await user.save()
    return user


@app.get("/get_inventory/", response_model=PaginateModel[InventoryItemDto],)
async def read_inventory(
    page: int,
    per_page: int,
    Part_sub_commodity: str = "",
    Product: str = "",
    location: str = "",
    user: UserModel = Depends(get_current_user),
) -> PaginateModel[InventoryModel]:
    try:
        query = InventoryModel.objects.filter(user=user.id)
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


@app.get("/Provutement_mode_bar_chart_data", response_model=ProcurementBarChartModelDto)
async def get_procurement_mode_summary(user: UserModel = Depends(get_current_user)) -> ProcurementBarChartModelDto:
    Order_specific_part_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'Order specific'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    kanban_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'kanban'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    MRP_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Procurement mode" = 'MRP'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    Order_specific_part_result = await base_ormar_config.database.fetch_one(Order_specific_part_query)
    print(Order_specific_part_result)
    kanban_query_result = await base_ormar_config.database.fetch_one(kanban_query)
    print(kanban_query_result)
    MRP_query_result = await base_ormar_config.database.fetch_one(MRP_query)
    print(MRP_query_result)

    return ProcurementBarChartModelDto(Order_specific_sum=Order_specific_part_result["Total_Stock_Value"] if Order_specific_part_result else 0, kanban_sum=kanban_query_result["Total_Stock_Value"] if kanban_query_result else 0, MRP_sum=MRP_query_result["Total_Stock_Value"] if MRP_query_result else 0, user_id=user.id)


@app.get("/coverage_class_bar_chart_data", response_model=CoverageClassBarChartModelDto)
async def get_coverage_class_summary(user: UserModel = Depends(get_current_user)) -> CoverageClassBarChartModelDto:
    Order_specific_part_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = 'Order specific'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    fifteen_Days_query = f"""
        SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<15 days'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    thirty_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<30 days'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    ninety_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '<90 days'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    ninety_plus_Days_query = f"""
    SELECT SUM("Stock Value") AS Total_Stock_Value
    FROM my_table
    WHERE "Coverage class" = '>90 days'
    AND "Stock Value" IS NOT NULL
    AND user = {user.id};
    """
    Order_specific_part_result = await base_ormar_config.database.fetch_one(Order_specific_part_query)
    print(Order_specific_part_result)
    fifteen_Days_result = await base_ormar_config.database.fetch_one(fifteen_Days_query)
    print(fifteen_Days_result)
    thirty_Days_result = await base_ormar_config.database.fetch_one(thirty_Days_query)
    ninety_Days_result = await base_ormar_config.database.fetch_one(ninety_Days_query)
    ninety_plus_Days_result = await base_ormar_config.database.fetch_one(ninety_plus_Days_query)

    return CoverageClassBarChartModelDto(Order_specific_sum=Order_specific_part_result["Total_Stock_Value"] if Order_specific_part_result else 0, fifteen_Days_sum=fifteen_Days_result["Total_Stock_Value"] if fifteen_Days_result else 0, thirty_Days_sum=thirty_Days_result["Total_Stock_Value"] if thirty_Days_result else 0, ninety_Days_sum=ninety_Days_result["Total_Stock_Value"] if ninety_Days_result else 0, ninety_plus_Days_sum=ninety_plus_Days_result["Total_Stock_Value"] if ninety_plus_Days_result else 0, user_id=user.id)


@app.post("/inventory/upload")
async def upload_inventory(file: UploadFile = File(...), user: UserModel = Depends(get_current_user)):

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
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    userID = user.id
    cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `user` INTEGER")
    update_query = f"""
        UPDATE `{table_name}`
        SET `user` = {userID}
    """
    cursor.execute(update_query)
    conn.commit()
    conn.close()

    return {"message": "File uploaded and processed successfully"}

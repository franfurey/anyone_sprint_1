# src.transform.py
import numpy as np
import pandas as pd
from enum import Enum
from sqlalchemy import text
from dataclasses import dataclass
from collections import namedtuple
from pandas import DataFrame, read_sql
from typing import Callable, Dict, List
from sqlalchemy.engine.base import Engine

from src.config import QUERIES_ROOT_PATH

QueryResult = namedtuple("QueryResult", ["query", "result"])

class QueryEnum(Enum):
    """This class enumerates all the queries that are available"""

    DELIVERY_DATE_DIFFERECE = "delivery_date_difference"
    GLOBAL_AMMOUNT_ORDER_STATUS = "global_ammount_order_status"
    REVENUE_BY_MONTH_YEAR = "revenue_by_month_year"
    REVENUE_PER_STATE = "revenue_per_state"
    TOP_10_LEAST_REVENUE_CATEGORIES = "top_10_least_revenue_categories"
    TOP_10_REVENUE_CATEGORIES = "top_10_revenue_categories"
    REAL_VS_ESTIMATED_DELIVERED_TIME = "real_vs_estimated_delivered_time"
    ORDERS_PER_DAY_AND_HOLIDAYS_2017 = "orders_per_day_and_holidays_2017"
    GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP = "get_freight_value_weight_relationship"


def read_query(query_name: str) -> str:
    """Read the query from the file.

    Args:
        query_name (str): The name of the file.

    Returns:
        str: The query.
    """
    with open(f"{QUERIES_ROOT_PATH}/{query_name}.sql", "r") as f:
        sql_file = f.read()
        sql = text(sql_file)
    return sql


def query_delivery_date_difference(database: Engine) -> QueryResult:
    """Get the query for delivery date difference.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for delivery date difference.
    """
    query_name = QueryEnum.DELIVERY_DATE_DIFFERECE.value
    query = read_query(QueryEnum.DELIVERY_DATE_DIFFERECE.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_global_ammount_order_status(database: Engine) -> QueryResult:
    """Get the query for global amount of order status.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for global percentage of order status.
    """
    query_name = QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value
    query = read_query(QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_revenue_by_month_year(database: Engine) -> QueryResult:
    """Get the query for revenue by month year.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for revenue by month year.
    """
    query_name = QueryEnum.REVENUE_BY_MONTH_YEAR.value
    query = read_query(QueryEnum.REVENUE_BY_MONTH_YEAR.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_revenue_per_state(database: Engine) -> QueryResult:
    """Get the query for revenue per state.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for revenue per state.
    """
    query_name = QueryEnum.REVENUE_PER_STATE.value
    query = read_query(QueryEnum.REVENUE_PER_STATE.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_top_10_least_revenue_categories(database: Engine) -> QueryResult:
    """Get the query for top 10 least revenue categories.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for top 10 least revenue categories.
    """
    query_name = QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value
    query = read_query(QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_top_10_revenue_categories(database: Engine) -> QueryResult:
    """Get the query for top 10 revenue categories.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for top 10 revenue categories.
    """
    query_name = QueryEnum.TOP_10_REVENUE_CATEGORIES.value
    query = read_query(QueryEnum.TOP_10_REVENUE_CATEGORIES.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_real_vs_estimated_delivered_time(database: Engine) -> QueryResult:
    """Get the query for real vs estimated delivered time.

    Args:
        database (Engine): Database connection.

    Returns:
        Query: The query for real vs estimated delivered time.
    """
    query_name = QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value
    query = read_query(QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value)
    return QueryResult(query=query_name, result=read_sql(query, database))


def query_freight_value_weight_relationship(database: Engine) -> QueryResult:
    """Get the freight_value weight relation for delivered orders.

    In this particular query, we want to evaluate if exists a correlation between
    the weight of the product and the value paid for delivery.

    We will use olist_orders, olist_order_items, and olist_products tables alongside
    some Pandas magic to produce the desired output: A table that allows us to
    compare the order total weight and total freight value.

    Of course, you could also do this with pure SQL statements but we would like
    to see if you've learned correctly the pandas' concepts seen so far.

    Args:
        database (Engine): Database connection.

    Returns:
        QueryResult: The query for freight_value vs weight data.
    """
    query_name = QueryEnum.GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP.value

    # Get orders from olist_orders table
    orders = read_sql(sql="SELECT * FROM olist_orders", con=database)

    # Get items from olist_order_items table
    items = read_sql(sql="SELECT * FROM olist_order_items", con=database)

    # Get products from olist_products table
    products = read_sql(sql="SELECT * FROM olist_products", con=database)

    # Merge items and products tables on 'product_id'
    items_products = pd.merge(items, products, on='product_id')

    # Merge orders with the result of items_products on 'order_id'
    data = pd.merge(orders, items_products, on='order_id')

    # Filter only delivered orders
    delivered = data.query("order_status == 'delivered'")

    aggregations = delivered.groupby('order_id').agg(
        freight_value=('freight_value', 'sum'),
        product_weight_g=('product_weight_g', 'sum')
    ).reset_index()

    return QueryResult(query=query_name, result=aggregations)

@dataclass
class QueryResult:
    query: str
    result: any

def query_orders_per_day_and_holidays_2017(database: Engine) -> QueryResult:
    """Get the query for orders per day and holidays in 2017.
    This function fetches data on the number of orders per day for the year 2017 and marks which days are holidays.

    Args:
        database (Engine): Database connection.

    Returns:
        QueryResult: The result of the query with order count, date, and holiday indicator.
    """
    query_name = "orders_per_day_and_holidays_2017"
    
    # Reading the public holidays from public_holidays table for 2017 only
    holidays = read_sql(sql="SELECT date FROM public_holidays WHERE strftime('%Y', date) = '2017'", con=database)
    holidays['date'] = pd.to_datetime(holidays['date'])  # Convert to date format

    # Reading the orders from olist_orders table
    orders = read_sql(sql="SELECT order_purchase_timestamp FROM olist_orders", con=database)
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])  # Convert to datetime

    # Filter orders from 2017 and group by date
    orders['date'] = orders['order_purchase_timestamp'].dt.date
    order_counts = orders[orders['order_purchase_timestamp'].dt.year == 2017].groupby('date').size().reset_index(name='order_count')

    order_counts['date'] = pd.to_datetime(order_counts['date'])
    holidays['date'] = pd.to_datetime(holidays['date'])
    
    result = pd.merge(order_counts, holidays, how='left', on='date')
    result['holiday'] = result['date'].isin(holidays['date'])
    result['holiday'].fillna(False, inplace=True)

    result['date'] = (result['date'].astype(np.int64) // 10 ** 6)

    return QueryResult(query=query_name, result=result)

def get_all_queries() -> List[Callable[[Engine], QueryResult]]:
    """Get all queries.

    Returns:
        List[Callable[[Engine], QueryResult]]: A list of all queries.
    """
    return [
        query_delivery_date_difference,
        query_global_ammount_order_status,
        query_revenue_by_month_year,
        query_revenue_per_state,
        query_top_10_least_revenue_categories,
        query_top_10_revenue_categories,
        query_real_vs_estimated_delivered_time,
        query_orders_per_day_and_holidays_2017,
        query_freight_value_weight_relationship,
    ]


def run_queries(database: Engine) -> Dict[str, DataFrame]:
    """Transform data based on the queries. For each query, the query is executed and
    the result is stored in the dataframe.

    Args:
        database (Engine): Database connection.

    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the query file names and
        values the result of the query as a dataframe.
    """
    query_results = {}
    for query in get_all_queries():
        query_result = query(database)
        query_results[query_result.query] = query_result.result
    return query_results

from fastapi import FastAPI, Depends, HTTPException, status, Request, requests
from google.cloud import bigquery
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

PROJECT_ID = "project-9eaeb0d6-fda0-4e8b-84f"

DATASET = "property_mgmt"


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# ---------------------------------------------------------------------------
# id_list
# ---------------------------------------------------------------------------

def id_list(bq: bigquery.Client = Depends(get_bq_client)):
    
    """
    finds property_id list for incorrect input handling
    """
    
    
    id_list = []

    query = f"""
    SELECT DISTINCT
        property_id
    FROM `property_mgmt.properties`
    """
    
    results = bq.query(query).result()
    
    for row in results:
        id_list.append(row[0])

    return id_list

def income_id_list(bq: bigquery.Client = Depends(get_bq_client)):
    
    """
    finds income_id list for incorrect input handling
    """
    
    
    income_id_list = []

    query = f"""
    SELECT DISTINCT
        income_id
    FROM `property_mgmt.income`
    """
    
    results = bq.query(query).result()
    
    for row in results:
        income_id_list.append(row[0])

    return income_id_list
# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """ 
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `property_mgmt.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties
    

@app.get("/properties/{property_id}")
def get_ind_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a specific property by its ID.
    """

    if property_id not in id_list(bq):
        error = f"invalid property_id, please try again"
        return error
    
    query =  f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `property_mgmt.properties`
        WHERE property_id = {property_id}
    """

    try:
        result = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    property = [dict(row) for row in result]
    return property

@app.get("/income/{property_id}")
def get_ind_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    returns income for a specified property
    """

    if property_id not in id_list(bq):
        error = f"invalid property_id, please try again"
        return error

    #find all income records from the income table based on a property_id
    query = f"""
    SELECT
        property_id,
        income_id,
        amount,
        date,
        description
    FROM `property_mgmt.income`
    Where property_id = {property_id}
    ORDER BY income_id
    """
    try:
        result = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    income = [dict(row) for row in result]
    return income

class IncomeRecord(BaseModel):
    income_id: int
    amount: float
    date: str
    description: str


@app.post("/income/{property_id}")
async def add_income_rec(property_id: int, record: IncomeRecord, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Adds a new income record to a specific property based on property_id 
    """ 

    #check for correct property_id
    if property_id not in id_list(bq): 
        error = f"invalid property_id, please try again" 
        return error 
    
    try: 
        #retrieve correct table
        table_id = "project-9eaeb0d6-fda0-4e8b-84f.property_mgmt.income"
        #retrieve property_id from url
        insert = record.model_dump()
        insert["property_id"] = property_id

        #insert data into the new row
        new_row = [insert]

        errors = bq.insert_rows_json(table_id, new_row)
        
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")
    
    except Exception as e: 
        raise HTTPException(status_code=500, detail=f"Database action failed: {str(e)}")
    
    result = f"income record recieved for {property_id}" 
    return result   

@app.get("/expenses/{property_id}")
def get_ind_expense(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    returns expense records for an individual property based on property_id
    """

    if property_id not in id_list(bq):
        error = f"invalid property_id, please try again"
        return error



    query = f"""
    SELECT
        *
    FROM property_mgmt.expenses
    WHERE property_id = {property_id}
    ORDER BY expense_id
    """

    try:
        result = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    expenses = [dict(row) for row in result]
    return expenses

class ExpenseRecord(BaseModel):
    expense_id: int
    amount: float
    date: str
    category: str
    vendor: str
    description: str


@app.post("/expense/{property_id}")
async def add_expense_rec(property_id: int, record: ExpenseRecord, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Adds a new expense record to a specific property based on property_id 
    """ 

    #check for correct property_id
    if property_id not in id_list(bq): 
        error = f"invalid property_id, please try again" 
        return error 
    
    try: 
        #retrieve correct table
        table_id = "project-9eaeb0d6-fda0-4e8b-84f.property_mgmt.expenses"
        #retrieve property_id from url
        insert = record.model_dump()
        insert["property_id"] = property_id

        #insert data into the new row
        new_row = [insert]

        errors = bq.insert_rows_json(table_id, new_row)
        
        if errors:
            raise Exception(f"BigQuery insert errors: {errors}")
    
    except Exception as e: 
        raise HTTPException(status_code=500, detail=f"Database action failed: {str(e)}")
    
    result = f"expense record recieved for {property_id}" 
    return result   


@app.get('/profit')
def profit(bq: bigquery.Client = Depends(get_bq_client)):
    """
    finds profit of all operations
    """


    query = f"""
    SELECT
        sum(income.amount) - sum(expense.amount)
    FROM `property_mgmt.income` as income
    FULL OUTER join `property_mgmt.expenses` as expense on income.property_id = expense.property_id
    """

    try:
        result = bq.query(query).result()

    except Exception as e:
        raise  HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    profit = [dict(row) for row in result]

    something = f"profit of all operations is {profit[0]['f0_']}"
    return something




@app.get('/profit/{property_id}')
def profit_ind(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    finds profit of a property
    sum of all income(amount) minus sum of all expense(amount)
    """

    if property_id not in id_list(bq):
        error = f"invalid property_id, please try again"
        return error

    query = f"""
    SELECT
        sum(income.amount) - sum(expense.amount)
    FROM `property_mgmt.income` as income
    FULL OUTER join `property_mgmt.expenses` as expense on income.property_id = expense.property_id
    WHERE income.property_id = {property_id}
    """

    try:
        result = bq.query(query).result()

    except Exception as e:
        raise  HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    profit = [dict(row) for row in result]

    something = f"profit of property {property_id} is {profit[0]['f0_']}"
    return something

@app.delete('/delete/{income_id}')
def delete_income_record(income_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    deletes an income record based on income_id
    """

    #get query
    query = f"""
    DELETE FROM `property_mgmt.income`
    WHERE income_id = {income_id}
    """

    try:
        result = bq.query(query).result()

        if result.num_dml_affected_rows == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Income record {income_id} was not found in the database during deletion."
            )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"row deletion failed: {str(e)}"
        )

    success = f"income record {income_id} successfully deleted"
    return success

@app.delete('/delete/{expense_id}')
def delete_expense_record(expense_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    deletes an expense record based on income_id
    """

    #get query
    query = f"""
    DELETE FROM `property_mgmt.expenses`
    WHERE income_id = {expense_id}
    """

    try:
        result = bq.query(query).result()

        if result.num_dml_affected_rows == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Expense record {expense_id} was not found in the database during deletion."
            )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"row deletion failed: {str(e)}"
        )

    success = f"Expense record {expense_id} successfully deleted"
    return success

        

        

    
    

from fastapi import FastAPI, Depends, HTTPException, status, Request
from google.cloud import bigquery
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

PROJECT_ID = "project-9eaeb0d6-fda0-4e8b-84f"

DATASET = "property_mgmt"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["get", "post", "delete", "put"],
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
    finds property_id list for incorrect input handling
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

@app.post("/income/record/{property_id}")
async def add_income_rec(property_id: int, request: Request, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Adds a new income record to a specific property based on property_id
    """

    if property_id not in id_list(bq):
        error = f"invalid property_id, please try again"
        return error

    body = await request.json()

    income_id = body.get("income_id")
    amount = body.get("amount")
    date = body.get("date")
    description = body.get("description")

    if income_id is None or amount is None or date is None or description is None:
        error = "missing input, income_id, amount, date, and description are all required"
        return error

    if income_id in income_id_list(bq):
            error = f"income_id already exists, please try again"
            return error
    try:
       response = requests.post(url, json=data)
    
    except Exception as e:
        raise HTTPException(status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail = f"Database query failed: {str(e)}")
    
    result = f"income record recieved for {property_id}, income record {income_id}"
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

#@app.post("/expenses/{property_id}")
#def add_expense_rec(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
#    """
#   
# if property_id not in id_list(bq):
#        error = f"invalid property_id, please try again"
#        return error
# 
#  adds a new expense record for a specific property based on property_id
#    """


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


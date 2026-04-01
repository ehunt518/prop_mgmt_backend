from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = "project-9eaeb0d6-fda0-4e8b-84f"

DATASET = "property_mgmt"


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

#@app.post("/income/{property_id}")
#def add_income_rec(property_id = int, bq: bigquery.Client = Depends(get_bq_client)):
#    """
#    Adds a new income record to a specific property based on property_id
#    """
#
#    #create body when you understand how to


@app.get("/expenses/{property_id}")
def get_ind_expense(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    returns expense records for an individual property based on property_id
    """
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
#    adds a new expense record for a specific property based on property_id
#    """


@app.get('/profit/{property_id}')
def profit(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    finds profit of a property
    sum of all income(amount) minus sum of all expense(amount)
    """

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

    if profit[0]['f0_'] == None:

        something = "invalid property_id"

    else:
        something = f"profit of {property_id}, is {profit[0]['f0_']}"
    return something

@app.get("/property_id/")
def test(bq: bigquery.Client = Depends(get_bq_client)):
    """
    test
    """

    query = f"""
    SELECT
        property_id
    FROM `property_mgmt.properties`
    """

    try:
        result = bq.query(query).result()
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    id_list = [dict(row) for row in result]
    return id_list
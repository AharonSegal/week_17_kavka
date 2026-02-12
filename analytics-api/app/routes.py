from fastapi import APIRouter

from dal import top_customers,customers_without_orders,zero_credit_active_customers

router = APIRouter()

@router.get("/analytics/top-customers")
def top_customers_route():
    rows = top_customers()

    result = []
    for row in rows:
        result.append(
            {
                "customerNumber": row[0],
                "customerName": row[1],
                "ordersCount": int(row[2]),
            }
        )
    return result

@router.get("/analytics/customers-without-orders")
def customers_without_orders_route():
    rows = customers_without_orders()
    result = []
    for row in rows:
        result.append({"customerNumber": row[0], "customerName": row[1]})
    return result


@router.get("/analytics/zero-credit-active-customers")
def zero_credit_active_customers():
    rows  = zero_credit_active_customers()
    result = []
    for row in rows:
        result.append(
            {
                "customerNumber": row[0],
                "customerName": row[1],
                "creditLimit": float(row[2]),
                "ordersCount": int(row[3]),
            }
        )
    return result
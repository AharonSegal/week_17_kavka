"""
  {
    type: customer,
    customerNumber: 151,
    customerName: Muscle Machine Inc,
    contactLastName: Young,
    contactFirstName: Jeff,
    phone: 2125557413,
    addressLine1: 4092 Furth Circle,
    addressLine2: Suite 400,
    city: NYC,
    state: NY,
    postalCode: 10022,
    country: USA,
    salesRepEmployeeNumber: 1286,
    creditLimit: 138500.00
  },
  {
    type: order,
    orderNumber: 10122,
    orderDate: 2003-05-08,
    requiredDate: 2003-05-16,
    shippedDate: 2003-05-13,
    status: Shipped,
    comments: null,
    customerNumber: 350
  },
"""

from pydantic import BaseModel
from typing import Optional

class CustomerModel(BaseModel):

    type: str
    customerNumber: int
    customerName: Optional[str]
    contactLastName: Optional[str]
    contactFirstName: Optional[str]
    phone: Optional[str]
    addressLine1: Optional[str]
    addressLine2: Optional[str]
    city: Optional[str]
    state: Optional[str]
    postalCode: Optional[str]
    country: Optional[str]
    salesRepEmployeeNumber: Optional[int]
    creditLimit: Optional[str]


class OrderModel(BaseModel):

    type: str
    orderNumber: int
    orderDate: Optional[str]
    requiredDate: Optional[str]
    shippedDate: Optional[str]
    status: Optional[str]
    comments: Optional[str]
    customerNumber: int



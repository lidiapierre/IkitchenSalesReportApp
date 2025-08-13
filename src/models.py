from pydantic import BaseModel
from typing import Optional, List

class Customer(BaseModel):
    customer_id: Optional[str] = None
    name: Optional[str]
    phone_number: str
    email: Optional[str]
    address: Optional[str]
    company_name: Optional[str] = None
    is_VIP: bool = False



class OrderItem(BaseModel):
    item_name: str
    quantity: float
    amount: float


class Order(BaseModel):
    order_id: str
    customer_id: Optional[str] = None
    order_date: str
    order_items: List[OrderItem]
    order_items_text: str
    total_amount: float
    order_type: str
    receipt_id: str
    location: str

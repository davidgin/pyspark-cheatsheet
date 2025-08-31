from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal
from datetime import datetime, date
from enum import Enum

class Department(str, Enum):
    """Valid department values"""
    ENGINEERING = "Engineering"
    SALES = "Sales"
    MARKETING = "Marketing"
    HR = "HR"
    FINANCE = "Finance"

class OrderStatus(str, Enum):
    """Valid order status values"""
    PENDING = "pending"
    SHIPPED = "shipped"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class ActionType(str, Enum):
    """Valid user action types"""
    VIEW = "view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    REMOVE_FROM_CART = "remove_from_cart"

class CustomerModel(BaseModel):
    """Pydantic model for customer data validation"""
    customer_id: int = Field(..., gt=0, description="Unique customer identifier")
    name: str = Field(..., min_length=1, max_length=100, description="Customer name")
    age: Optional[int] = Field(None, ge=18, le=120, description="Customer age")
    city: Optional[str] = Field(None, max_length=50, description="Customer city")
    salary: Optional[Decimal] = Field(None, ge=0, description="Customer salary")
    department: Optional[Department] = Field(None, description="Customer department")
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError('Name cannot be empty or whitespace')
        return v.strip().title()
    
    @field_validator('city')
    @classmethod
    def validate_city(cls, v):
        if v:
            return v.strip().title()
        return v
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str
        }

class OrderModel(BaseModel):
    """Pydantic model for order data validation"""
    order_id: int = Field(..., gt=0, description="Unique order identifier")
    customer_id: int = Field(..., gt=0, description="Customer who placed the order")
    product_name: str = Field(..., min_length=1, max_length=100, description="Product name")
    quantity: int = Field(..., gt=0, description="Quantity ordered")
    price: Decimal = Field(..., gt=0, description="Unit price")
    order_date: Optional[date] = Field(None, description="Order date")
    status: Optional[OrderStatus] = Field(OrderStatus.PENDING, description="Order status")
    
    @field_validator('product_name')
    @classmethod
    def validate_product_name(cls, v):
        return v.strip().title()
    
    @field_validator('price')
    @classmethod
    def validate_price(cls, v):
        if v <= 0:
            raise ValueError('Price must be positive')
        return round(v, 2)
    
    @model_validator(mode='after')
    def validate_order_data(self):
        if self.quantity and self.price:
            total_value = self.quantity * self.price
            if total_value > 10000:  # Business rule: max order value
                raise ValueError(f'Order total ${total_value} exceeds maximum allowed ($10,000)')
        return self
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            date: lambda v: v.isoformat()
        }

class UserActivityModel(BaseModel):
    """Pydantic model for user activity data validation"""
    timestamp: datetime = Field(..., description="Activity timestamp")
    user_id: int = Field(..., gt=0, description="User identifier")
    action: ActionType = Field(..., description="Type of action performed")
    product: Optional[str] = Field(None, max_length=100, description="Product involved")
    value: Optional[Decimal] = Field(None, ge=0, description="Action value")
    
    @field_validator('product')
    @classmethod
    def validate_product(cls, v):
        if v:
            return v.strip().lower()
        return v
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }

class TransactionModel(BaseModel):
    """Pydantic model for transaction data validation"""
    event_time: datetime = Field(..., description="Transaction timestamp")
    user_id: int = Field(..., gt=0, description="User identifier")
    product_id: str = Field(..., min_length=1, description="Product identifier")
    amount: Decimal = Field(..., gt=0, description="Transaction amount")
    city: Optional[str] = Field(None, max_length=50, description="Transaction city")
    
    @field_validator('product_id')
    @classmethod
    def validate_product_id(cls, v):
        return v.strip().upper()
    
    @field_validator('city')
    @classmethod
    def validate_city(cls, v):
        if v:
            return v.strip().title()
        return v
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }

class LiveDataModel(BaseModel):
    """Pydantic model for live streaming data validation"""
    timestamp: datetime = Field(..., description="Event timestamp")
    user_id: int = Field(..., gt=0, description="User identifier")
    action: ActionType = Field(..., description="Action performed")
    value: Optional[Decimal] = Field(None, ge=0, description="Action value")
    session_id: str = Field(..., min_length=1, description="Session identifier")
    
    @field_validator('session_id')
    @classmethod
    def validate_session_id(cls, v):
        if not v.startswith('session_'):
            raise ValueError('Session ID must start with "session_"')
        return v
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }

class NestedUserInfo(BaseModel):
    """Nested user information model"""
    user_id: int = Field(..., gt=0)
    name: str = Field(..., min_length=1)
    email: Optional[str] = Field(None, pattern=r'^[\w\.-]+@[\w\.-]+\.\w+$')
    preferences: Optional[Dict[str, Any]] = None

class NestedOrderItem(BaseModel):
    """Nested order item model"""
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    price: Decimal = Field(..., gt=0)

class NestedOrder(BaseModel):
    """Nested order model"""
    order_id: int = Field(..., gt=0)
    items: List[NestedOrderItem] = Field(..., min_items=1) # type: ignore
    order_date: datetime
    total: Decimal = Field(..., gt=0)

class NestedDataModel(BaseModel):
    """Complex nested data structure model"""
    id: int = Field(..., gt=0)
    user_info: NestedUserInfo
    orders: Optional[List[NestedOrder]] = None
    metadata: Optional[Dict[str, str]] = None

class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass

class DataValidator:
    """Utility class for validating data using Pydantic models"""
    
    @staticmethod
    def validate_customer_data(data: Dict[str, Any]) -> CustomerModel:
        """Validate customer data"""
        try:
            return CustomerModel(**data)
        except Exception as e:
            raise DataValidationError(f"Customer validation failed: {e}")
    
    @staticmethod
    def validate_order_data(data: Dict[str, Any]) -> OrderModel:
        """Validate order data"""
        try:
            return OrderModel(**data)
        except Exception as e:
            raise DataValidationError(f"Order validation failed: {e}")
    
    @staticmethod
    def validate_user_activity_data(data: Dict[str, Any]) -> UserActivityModel:
        """Validate user activity data"""
        try:
            return UserActivityModel(**data)
        except Exception as e:
            raise DataValidationError(f"User activity validation failed: {e}")
    
    @staticmethod
    def validate_transaction_data(data: Dict[str, Any]) -> TransactionModel:
        """Validate transaction data"""
        try:
            return TransactionModel(**data)
        except Exception as e:
            raise DataValidationError(f"Transaction validation failed: {e}")
    
    @staticmethod
    def validate_batch_data(data_list: List[Dict[str, Any]], 
                           model_class: BaseModel) -> List[BaseModel]:
        """Validate a batch of data records"""
        validated_records = []
        errors = []
        
        for i, record in enumerate(data_list):
            try:
                validated_record = model_class(**record)
                validated_records.append(validated_record)
            except Exception as e:
                errors.append(f"Record {i}: {e}")
        
        if errors:
            raise DataValidationError(f"Batch validation failed with {len(errors)} errors: {errors[:5]}")
        
        return validated_records
    
    @staticmethod
    def get_validation_summary(data_list: List[Dict[str, Any]], 
                              model_class: BaseModel) -> Dict[str, Any]:
        """Get validation summary without raising exceptions"""
        total_records = len(data_list)
        valid_records = []
        invalid_records = []
        
        for i, record in enumerate(data_list):
            try:
                validated_record = model_class(**record)
                valid_records.append((i, validated_record))
            except Exception as e:
                invalid_records.append((i, str(e)))
        
        return {
            "total_records": total_records,
            "valid_count": len(valid_records),
            "invalid_count": len(invalid_records),
            "validation_rate": len(valid_records) / total_records if total_records > 0 else 0,
            "valid_records": valid_records,
            "invalid_records": invalid_records[:10]  # Limit to first 10 errors
        }

def main():
    """Demonstrate Pydantic models usage"""
    print("PySpark with Pydantic Data Models")
    print("=" * 40)
    
    # Example customer data
    customer_data = {
        "customer_id": 1,
        "name": "  alice johnson  ",
        "age": 25,
        "city": "new york",
        "salary": 75000.50,
        "department": "Engineering"
    }
    
    try:
        customer = DataValidator.validate_customer_data(customer_data)
        print("[SUCCESS] Customer validation successful:")
        print(f"   Name: {customer.name}")
        print(f"   City: {customer.city}")
        print(f"   Department: {customer.department}")
        print(f"   JSON: {customer.json()}")
    except DataValidationError as e:
        print(f"[ERROR] Customer validation failed: {e}")
    
    # Example order data with business rule validation
    order_data = {
        "order_id": 1001,
        "customer_id": 1,
        "product_name": "  laptop  ",
        "quantity": 2,
        "price": 1200.999,
        "status": "completed"
    }
    
    try:
        order = DataValidator.validate_order_data(order_data)
        print("\n[SUCCESS] Order validation successful:")
        print(f"   Product: {order.product_name}")
        print(f"   Price: ${order.price}")
        print(f"   Total: ${order.quantity * order.price}")
    except DataValidationError as e:
        print(f"\n[ERROR] Order validation failed: {e}")
    
    print("\n" + "=" * 40)
    print("Pydantic models ready for use with PySpark!")

if __name__ == "__main__":
    main()
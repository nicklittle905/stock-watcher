from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional


@dataclass
class ProductState:
    vendor: str
    url: str
    product_name: Optional[str]
    price: Optional[Decimal]
    currency: Optional[str]
    in_stock: Optional[bool]
    checked_at: datetime
    image_url: Optional[str] = None
    raw_meta: Dict[str, Any] = field(default_factory=dict)
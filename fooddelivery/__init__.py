# fooddelivery_datacleaning/__init__.py

"""
Food Delivery Data Cleaning Service
-----------------------------------

This package provides:
- Config management (config.py)
- Database utilities (db.py)
- Data processing & cleaning (processing.py)
- Pandera schema validations (schemas.py)
"""

__version__ = "0.1.0"

# Only expose modules we expect users to import directly
__all__ = ["config", "db", "processing", "schemas"]

"""Omni BI number and date format codes.

Reference: docs.omni.co/modeling/dimensions/parameters/format

Decimal suffix convention: append _0 through _4 to control decimal places.
E.g., USDCURRENCY_0 = "$1,235", USDCURRENCY_2 = "$1,234.50".
"""

# -- Number formats --
NUMBER = "number"  # 1,234.50
BIG_NUMBER = "big"  # 5.60M, 1.23K
ID = "id"  # 1234 (no formatting)

# -- Currency --
USD = "usdcurrency"  # $1,234.50
USD_BIG = "bigusdcurrency"  # $5.60M
EUR = "eurcurrency"  # 1.234,50 EUR
GBP = "gbpcurrency"  # GBP 1,234.50
ACCOUNTING = "usdaccounting"  # $(1,234.50) for negatives
FINANCIAL = "financial"  # 1,234.50 (no symbol)

# -- Percentage --
PERCENT = "percent"  # 24.4%

# -- Shortcuts with common decimal suffixes --
NUMBER_0 = "number_0"  # 1,235
NUMBER_2 = "number_2"  # 1,234.50
BIG_NUMBER_0 = "big_0"  # 5.6M
BIG_NUMBER_2 = "big_2"  # 5.60M
USD_0 = "usdcurrency_0"  # $1,235
USD_2 = "usdcurrency_2"  # $1,234.50
USD_BIG_0 = "bigusdcurrency_0"  # $5.6M
USD_BIG_2 = "bigusdcurrency_2"  # $5.60M
PERCENT_0 = "percent_0"  # 24%
PERCENT_1 = "percent_1"  # 24.4%
PERCENT_2 = "percent_2"  # 24.40%

# All valid Omni format bases (append _0 through _4 for decimal control)
VALID_FORMAT_BASES = frozenset({
    "number", "big", "id", "usdcurrency", "bigusdcurrency",
    "eurcurrency", "gbpcurrency", "usdaccounting", "financial",
    "percent",
})

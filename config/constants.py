"""
Constants for data type sizes in bytes
"""

INTEGER_SIZE = 8  # bytes
NUMBER_SIZE = 8   # bytes (float/double)
STRING_SIZE = 80  # bytes
DATE_SIZE = 20    # bytes
LONG_STRING_SIZE = 200  # bytes

KEY_VALUE_OVERHEAD = 12  # bytes per key-value pair
ARRAY_OVERHEAD = 12      # bytes for array structure

TYPE_SIZES = {
    'integer': INTEGER_SIZE,
    'number': NUMBER_SIZE,
    'string': STRING_SIZE,
    'date': DATE_SIZE,
    'longstring': LONG_STRING_SIZE
}

# ATTENTION CONVERTIR EN OCTET/S ?

# Network and query execution constants
BANDWIDTH_SPEED = 15_000_000  
BANDWIDTH_SPEED_BYTES_PER_MS = BANDWIDTH_SPEED / 1000  # bytes per millisecond

# Cost constants
COST_PER_GB_TRANSFER = 0.01  # USD per GB transferred
CARBON_PER_GB_TRANSFER = 0.5  # gCO2 per GB transferred (approximate)
COST_PER_SERVER_MS = 0.0001  # USD per server millisecond
CARBON_PER_SERVER_MS = 0.001  # gCO2 per server millisecond

# Query execution constants
INDEX_ACCESS_TIME_MS = 0.1  # milliseconds per index access
FULL_SCAN_TIME_PER_DOC_MS = 0.001  # milliseconds per document in full scan
COMPARISON_TIME_MS = 0.0001  # milliseconds per comparison operation
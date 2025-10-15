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
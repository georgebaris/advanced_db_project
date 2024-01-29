# my_udfs.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from math import radians, sin, cos, sqrt, atan2

# Descent Mapping
descent_mapping = {
    'A': 'Asian', 'B': 'Black', 'C': 'Caucasian', 'D': 'Indian',
    'F': 'Filipino', 'G': 'German', 'H': 'Hispanic', 'I': 'Italian',
    'J': 'Japanese', 'K': 'Korean', 'L': 'Laotian', 'O': 'Other',
    'P': 'Pacific Islander', 'S': 'Samoan', 'U': 'Hawaiian',
    'V': 'Vietnamese', 'W': 'White', 'X': 'Unknown',
    'Z': 'Asian Indian', '-': 'Not Specified', None: 'Unknown'
}

# match code to descent
def map_descent(code):
    return descent_mapping.get(code, 'Unknown')

#calculate distance using coordinates
def get_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    radius = 6371.0
    return radius * c

# categorize time into day segments
def get_day_segment(time_str):
    try:
        hour = int(time_str['TIME OCC'][:2])

        # Classify into day segments
        if 5 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 21:
            return 'Evening'
        else:
            return 'Night'
    except ValueError:
        return 'Unknown'

# register udfs
map_descent_udf = udf(map_descent, StringType())
get_distance_udf = udf(get_distance, FloatType())
get_day_segment_udf = udf(get_day_segment, StringType())

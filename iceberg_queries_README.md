# Iceberg queries in data_access_layer

This guide describes how data_access_layer translates structured conditions dictionaries into PyIceberg BooleanExpressions. It lets a user define filters without importing the necessary PyIceberg objects. 

## General
Supports only basic and / or rule interaction (1 level deep). 


Supports the following PyIceberg operators: 
- EqualTo
- NotEqualTo
- GreaterThan
- LessThan
- GreaterThanOrEqual
- LessThanOrEqual
- StartsWith
- NotStartsWith
- IsNull
- NotNull
- In
- NotIn

### Naming conversion dictionary:

```python
string_to_pyiceberg_operator = {
    'eq': EqualTo,
    'gt': GreaterThan,
    'lt': LessThan,
    'gte': GreaterThanOrEqual,
    'lte': LessThanOrEqual,
    'starts_with': StartsWith,
    'not_starts_with': NotStartsWith,
    'is_null': IsNull,
    'not_null': NotNull,
    'in': In,
    'not_in': NotIn,
    'neq': NotEqualTo,
}
```

## Examples

### StartsWith - the equivalent of SQL's "like 'substring%'":

Suppose we want to retrieve all records where the name column contains "metric". Using direct PyIceberg objects -

```python
from pyiceberg.expressions import StartsWith
row_filter = StartsWith('name', 'metric')
```

is equivalent to 

```python
structured_conditions = {
    'and' : [
        'name' : {
            'starts_with' : 'metric'
        },
    ],
    'or' : []
}
```


### GreaterThan:

Let's assume our table has a column created_at that stores timestamps. We can retrieve all records created in the last 7 days using GreaterThan. Using direct PyIceberg objects -

```python
from pyiceberg.expressions import GreaterThan
import datetime
seven_days_ago = (
    datetime.datetime.utcnow() - datetime.timedelta(days=7)
    ).isoformat()
row_filter = GreaterThan('created_at', seven_days_ago)
```

is equivalent to 

```python
import datetime
seven_days_ago = (
    datetime.datetime.utcnow() - datetime.timedelta(days=7)
    ).isoformat()
structured_conditions = {
    'and' : [
        'created_at' : {
            'gt' : seven_days_ago
        },
    ],
    'or' : []
}
```

### Multiple conditions:

Imagine we have a category column, and we want to retrieve all records where category is "A" and value is greater than 20. Using direct PyIceberg objects -

```python
from pyiceberg.expressions import And, EqualTo, GreaterThan
row_filter = And(
            EqualTo('category', 'A'),
            GreaterThan('value', 20)
        )
```

is equivalent to 

```python
structured_conditions = {
    'and' : [
        'category' : {
            'eq' : 'A'
        },
        'value' : {
            'gt' : 20
        },
    ],
    'or' : []
}
```


### Find Null or Missing Values:

If a dataset has missing or null values in a column, we can use IsNull to find them. Using direct PyIceberg objects -

```python
from pyiceberg.expressions import IsNull
row_filter = IsNull('value')
```

is equivalent to 

```python
structured_conditions = {
    'and' : [
        'value' : {
            'isna' : [] # or any other value
        },
    ],
    'or' : []
    
}
```


### Query Using a List of IDs (IN Clause):

To retrieve records matching multiple IDs at once, use In. Using direct PyIceberg objects -

```python
from pyiceberg.expressions import In
row_filter = In('id', [2, 4, 6])
```

is equivalent to 

```python
structured_conditions = {
    'and' : [
        'id' : {
            'in' : [2,4,6] 
        },
    ],
    'or' : []
    
}
```


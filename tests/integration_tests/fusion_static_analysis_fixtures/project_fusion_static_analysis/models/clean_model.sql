select
    CONCAT(first_name, ' ', last_name) as full_name,
    TRIM(email) as email,
    COALESCE(phone, 'unknown') as phone,
    ANY_VALUE(status) as latest_status
from customers

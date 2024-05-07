select 
    C1 as customer_id,
    C2 as customer_first_name,
    C3 as customer_last_name,
    C4 as customer_work_company,
    C5 as customer_mailing_address,
    C6 as customer_mailing_city,
    C7 as customer_mailing_state,
    C8 as customer_mailing_country,
    C9 as customer_mailing_zipcode,
    C10 as customer_phone_number,
    C11 as customer_fax_number,
    C12 as customer_email_address,
    C13 as representative_employee_id
from {{ source('music', 'customer') }}
where C1 <> 'CustomerId'
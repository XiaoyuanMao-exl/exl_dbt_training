select 
    EmployeeId as employee_id,
    LastName as employee_last_name,
    FirstName as employee_first_name,
    Title as employee_title,
    ReportsTo as supervisor_employee_id,
    BirthDate as employee_birth_date,
    HireDate as employee_hired_date,
    Address as employee_address,
    City as employee_city,
    State as employee_state,
    Country as employee_country,
    PostalCode as employee_zipcode,
    Phone as employee_phone_number,
    Fax as employee_fax_number,
    Email as employee_email_address
from {{ source('music', 'employee') }}
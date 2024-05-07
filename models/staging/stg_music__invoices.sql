select 
    InvoiceId as invoice_id,
    CustomerId as customer_id,
    InvoiceDate as invoice_date_est,
    BillingAddress as billing_address,
    BillingCity as billing_city,
    BillingState as billing_state,
    BillingCountry as billing_country,
    BillingPostalCode as billing_zipcode,
    Total as invoice_revenue_usd
from {{ source('music', 'invoice') }}
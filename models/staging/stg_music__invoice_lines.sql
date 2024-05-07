select 
    InvoiceLineId as invoice_lineid,
    InvoiceId as invoice_id,
    TrackId as track_id,
    UnitPrice as invoice_lineitem_revenue_usd,
    Quantity as quantity_purchased
from {{ source('music', 'invoiceline') }}
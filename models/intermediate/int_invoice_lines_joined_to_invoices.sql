with invoice_lines as (
    select * from {{ ref('stg_music__invoice_lines') }}
),

invoices as (
    select * from {{ ref('stg_music__invoices') }}
),

joined as (
    select 
        invoice_lines.invoice_lineid,
        invoice_lines.invoice_id,
        invoice_lines.track_id,
        invoices.customer_id,
        invoices.invoice_date_est,
        invoices.billing_address,
        invoices.billing_city,
        invoices.billing_state,
        invoices.billing_country,
        invoices.billing_zipcode,
        invoice_lines.quantity_purchased,
        invoice_lines.invoice_lineitem_revenue_usd
    from invoice_lines
    left join invoices
        on invoice_lines.invoice_id = invoices.invoice_id
)

select * from joined
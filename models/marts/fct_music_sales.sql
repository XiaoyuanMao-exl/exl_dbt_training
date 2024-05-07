with customers_to_employees as (
    select * from {{ ref('int_customers_joined_to_employees') }}
),

invoice_lines_to_invoices as (
    select * from {{ ref('int_invoice_lines_joined_to_invoices') }}
),

tracks_to_albums_artists_genre_mediatype as (
    select * from {{ ref('int_tracks_joined_to_albums_artists_genre_mediatype') }}
),

joined as (
    select
        invoice_lines_to_invoices.invoice_lineid,
        invoice_lines_to_invoices.invoice_id,
        invoice_lines_to_invoices.track_id,
        tracks_to_albums_artists_genre_mediatype.album_id,
        tracks_to_albums_artists_genre_mediatype.mediatype_id,
        tracks_to_albums_artists_genre_mediatype.genre_id,
        invoice_lines_to_invoices.customer_id,
        customers_to_employees.employee_id,
        invoice_lines_to_invoices.invoice_date_est,
        invoice_lines_to_invoices.billing_state,
        invoice_lines_to_invoices.billing_country,
        invoice_lines_to_invoices.quantity_purchased,
        invoice_lines_to_invoices.invoice_lineitem_revenue_usd,
        tracks_to_albums_artists_genre_mediatype.track_name,
        tracks_to_albums_artists_genre_mediatype.track_composer,
        tracks_to_albums_artists_genre_mediatype.album_title as album_name,
        tracks_to_albums_artists_genre_mediatype.artist_name,
        tracks_to_albums_artists_genre_mediatype.mediatype_name,
        tracks_to_albums_artists_genre_mediatype.genre_name,
        tracks_to_albums_artists_genre_mediatype.track_length_ms,
        tracks_to_albums_artists_genre_mediatype.track_length_seconds,
        tracks_to_albums_artists_genre_mediatype.track_length_mins,
        tracks_to_albums_artists_genre_mediatype.bytes as Bytes,
        customers_to_employees.customer_mailing_state,
        customers_to_employees.customer_mailing_country,
        customers_to_employees.customer_email_address,
        customers_to_employees.employee_name,
        customers_to_employees.employee_title,
        customers_to_employees.employee_city,
        customers_to_employees.employee_state,
        customers_to_employees.employee_country
    from invoice_lines_to_invoices
    left join tracks_to_albums_artists_genre_mediatype
        on invoice_lines_to_invoices.track_id = tracks_to_albums_artists_genre_mediatype.track_id
    left join customers_to_employees
        on invoice_lines_to_invoices.customer_id = customers_to_employees.customer_id
)

select * from joined
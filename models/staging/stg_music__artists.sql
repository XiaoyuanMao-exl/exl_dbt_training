select 
    artistid as artist_id,
    name as artist_name
from {{ source('music', 'artist') }}
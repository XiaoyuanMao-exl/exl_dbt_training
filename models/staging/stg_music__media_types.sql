select 
    mediatypeid as mediatype_id,
    name as mediatype_name
from {{ source('music', 'mediatype') }}
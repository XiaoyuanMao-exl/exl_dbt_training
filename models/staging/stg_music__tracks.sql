select 
    TrackId as track_id,
    Name as track_name,
    AlbumId as album_id,
    MediaTypeId as mediatype_id,
    GenreId as genre_id,
    Composer as track_composer,
    Milliseconds as track_length_ms,
    Bytes as bytes,
    UnitPrice as unit_price,
    track_length_ms/1000 as track_length_seconds,
    track_length_seconds/60 as track_length_mins
from {{ source('music', 'track') }}
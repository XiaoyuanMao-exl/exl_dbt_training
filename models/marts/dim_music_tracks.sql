with tracks_to_albums_artists_genre_mediatype as (
    select * from {{ ref('int_tracks_joined_to_albums_artists_genre_mediatype') }}
),

music_tracks as (
    select
        tracks_to_albums_artists_genre_mediatype.track_id,
        tracks_to_albums_artists_genre_mediatype.album_id,
        tracks_to_albums_artists_genre_mediatype.mediatype_id,
        tracks_to_albums_artists_genre_mediatype.genre_id,
        tracks_to_albums_artists_genre_mediatype.track_name,
        tracks_to_albums_artists_genre_mediatype.track_composer,
        tracks_to_albums_artists_genre_mediatype.album_title,
        tracks_to_albums_artists_genre_mediatype.artist_name,
        tracks_to_albums_artists_genre_mediatype.mediatype_name,
        tracks_to_albums_artists_genre_mediatype.genre_name,
        tracks_to_albums_artists_genre_mediatype.track_length_ms,
        tracks_to_albums_artists_genre_mediatype.track_length_seconds,
        tracks_to_albums_artists_genre_mediatype.track_length_mins,
        tracks_to_albums_artists_genre_mediatype.bytes as Bytes,
        tracks_to_albums_artists_genre_mediatype.unit_price
    from tracks_to_albums_artists_genre_mediatype
)

select * from music_tracks
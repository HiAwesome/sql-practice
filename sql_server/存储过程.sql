create procedure spAlbumsFromArtist
    @ArtistName varchar(255)
AS
    SELECT AlbumName, ReleaseDate
    FROM Music.dbo.Albums
        INNER JOIN Music.dbo.Artists
        ON Albums.ArtistId = Artists.ArtistID
    WHERE Artists.ArtistName = @ArtistName;
GO
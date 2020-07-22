USE Music;

create table Artists
(
    ArtistID   int IDENTITY (1,1) NOT NULL PRIMARY KEY,
    ArtistName nvarchar(255)      NOT NULL,
    ArtistFrom date
);

CREATE TABLE Genres
(
    GenreId int IDENTITY (1,1) NOT NULL PRIMARY KEY,
    Genre   nvarchar(50)       NOT NULL
);

CREATE TABLE Albums
(
    AlbumId     int IDENTITY (1,1) NOT NULL PRIMARY KEY,
    AlbumName   nvarchar(255)      NOT NULL,
    ReleaseDate date               NOT NULL,
    ArtistId    int                NOT NULL,
    GenreId     int                NOT NULL

        CONSTRAINT FK_Albums_Artists FOREIGN KEY (ArtistId)
            REFERENCES dbo.Artists (ArtistId)
            ON DELETE NO ACTION
            ON UPDATE NO ACTION
);


CREATE TABLE Student
(
    s_id   int          NOT NULL,
    s_name nvarchar(50) NOT NULL,
    s_age  int          not null,
    s_sex  int          not null
);

CREATE TABLE Course
(
    c_id   int          NOT NULL,
    c_name nvarchar(50) NOT NULL,
    c_age  int          not null,
    c_sex  int          not null
);

CREATE TABLE Score
(
    s_id  int          NOT NULL,
    c_id  nvarchar(50) NOT NULL,
    score int          not null
);
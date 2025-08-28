-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'MovieReviews')
BEGIN
    CREATE DATABASE MovieReviews;
    PRINT 'Database MovieReviews created.';
END
ELSE
    PRINT 'Database MovieReviews already exists.';
GO

USE MovieReviews;
GO

-- Create Genres table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Genres]') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Genres (
        GenreID INT IDENTITY(1,1) PRIMARY KEY,
        GenreName NVARCHAR(100) NOT NULL
    );
    PRINT 'Table Genres created.';
END
ELSE
    PRINT 'Table Genres already exists.';
GO

-- Create Movies table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Movies]') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Movies (
        MovieID INT IDENTITY(1,1) PRIMARY KEY,
        Title NVARCHAR(255) NOT NULL,
        ReleaseYear INT CHECK (ReleaseYear >= 1888),
        DurationMinutes INT CHECK (DurationMinutes > 0),
        GenreID INT NOT NULL,
        FOREIGN KEY (GenreID) REFERENCES dbo.Genres(GenreID)
    );
    PRINT 'Table Movies created.';
END
ELSE
    PRINT 'Table Movies already exists.';
GO

-- Create Reviews table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Reviews]') AND type in (N'U'))
BEGIN
    CREATE TABLE dbo.Reviews (
        ReviewID INT IDENTITY(1,1) PRIMARY KEY,
        MovieID INT NOT NULL,
        ReviewerName NVARCHAR(100),
        Rating INT CHECK (Rating BETWEEN 1 AND 10),
        ReviewText NVARCHAR(MAX),
        ReviewDate DATE,
        FOREIGN KEY (MovieID) REFERENCES dbo.Movies(MovieID)
    );
    PRINT 'Table Reviews created.';
END
ELSE
    PRINT 'Table Reviews already exists.';
GO

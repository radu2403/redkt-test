CREATE TABLE [assessment].[GeographyDimension]
(
    [A] [nvarchar](100) NOT NULL,
    [K] [nvarchar](100) NOT NULL,
    [L] [nvarchar](100) NOT NULL,
    [M] integer NOT NULL,
    -- [M] Primary Key
)
WITH
(
    DISTRIBUTION = HASH([M]),
    CLUSTERED COLUMNSTORE INDEX
)

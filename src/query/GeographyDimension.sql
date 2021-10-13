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
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)

-- TODO
-- Any improvements that can be made here?
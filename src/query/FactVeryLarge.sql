CREATE TABLE [assessment].[FactVeryLarge]
(
    [C] [decimal](20, 5) NOT NULL,
    [D] integer NOT NULL,
    [E] integer NOT NULL,
    [F] integer NULL,
    [G] integer NULL,
    [H] integer NULL,
    [I] integer NULL,
    [J] integer NULL,
    [M] integer NOT NULL,
    -- [M] Surrogate Key to Geography Dimension -- Provided (GeographyDimension.sql)
    [P] integer NOT NULL,
    -- [P] Surrogate Key to Product Dimension -- Assume created and existing
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
)

-- TODO
-- Any improvements that can be made here?

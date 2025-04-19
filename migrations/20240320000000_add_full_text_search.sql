-- Add full-text search columns and indexes
ALTER TABLE ingredients 
ADD COLUMN search_vector tsvector 
GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(unit, '')), 'B')
) STORED;

CREATE INDEX ingredients_search_idx ON ingredients USING GIN (search_vector);

ALTER TABLE formulas 
ADD COLUMN search_vector tsvector 
GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(description, '')), 'B')
) STORED;

CREATE INDEX formulas_search_idx ON formulas USING GIN (search_vector); 
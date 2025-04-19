-- Enable pg_trgm extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add trigram indexes for fuzzy matching
CREATE INDEX ingredients_name_trgm_idx ON ingredients USING GIN (name gin_trgm_ops);
CREATE INDEX ingredients_unit_trgm_idx ON ingredients USING GIN (unit gin_trgm_ops);

CREATE INDEX formulas_name_trgm_idx ON formulas USING GIN (name gin_trgm_ops);
CREATE INDEX formulas_description_trgm_idx ON formulas USING GIN (description gin_trgm_ops);

-- Add similarity function to search_vector generation
ALTER TABLE ingredients 
ALTER COLUMN search_vector SET GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(unit, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(similarity(name, '')::text, '')), 'C') ||
    setweight(to_tsvector('english', coalesce(similarity(unit, '')::text, '')), 'D')
) STORED;

ALTER TABLE formulas 
ALTER COLUMN search_vector SET GENERATED ALWAYS AS (
    setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('english', coalesce(description, '')), 'B') ||
    setweight(to_tsvector('english', coalesce(similarity(name, '')::text, '')), 'C') ||
    setweight(to_tsvector('english', coalesce(similarity(description, '')::text, '')), 'D')
) STORED; 
-- Create ingredients table
CREATE TABLE IF NOT EXISTS ingredients (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    unit VARCHAR(50) NOT NULL,
    cost_per_unit DECIMAL(10,2) NOT NULL,
    density DECIMAL(10,2)
);

-- Create formulas table
CREATE TABLE IF NOT EXISTS formulas (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    ingredients JSONB NOT NULL,
    mass DECIMAL(10,2)
);

-- Create invoices table with JSONB ingredients
CREATE TABLE IF NOT EXISTS invoices (
    id UUID PRIMARY KEY,
    date DATE NOT NULL,
    supplier VARCHAR(255) NOT NULL,
    pdf_path TEXT NOT NULL,
    ingredients JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Drop the junction table if it exists
DROP TABLE IF EXISTS invoice_ingredients;

-- Add search vector column to formulas
ALTER TABLE formulas ADD COLUMN IF NOT EXISTS search_vector tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(name, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(description, '')), 'B')
    ) STORED;

-- Create search index
CREATE INDEX IF NOT EXISTS formulas_search_idx ON formulas USING GIN (search_vector); 
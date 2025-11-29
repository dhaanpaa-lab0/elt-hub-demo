CREATE TABLE dim_fiscal_year
(
    fiscal_year_id SERIAL PRIMARY KEY,
    fiscal_year    INTEGER NOT NULL,
    start_date     DATE    NOT NULL,
    end_date       DATE    NOT NULL,
    is_current_fy  BOOLEAN   DEFAULT FALSE,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fiscal_year_unique UNIQUE (fiscal_year),
    CONSTRAINT valid_dates CHECK (start_date < end_date),
    CONSTRAINT valid_fiscal_year CHECK (fiscal_year > 1900)
);

-- Create an index on commonly queried columns
CREATE INDEX idx_fiscal_year_dates ON dim_fiscal_year (start_date, end_date);

-- Create a trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_fiscal_year_timestamp()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_fiscal_year_timestamp
    BEFORE UPDATE
    ON dim_fiscal_year
    FOR EACH ROW
EXECUTE FUNCTION update_fiscal_year_timestamp();

-- Example insert for a fiscal year (adjust dates according to your fiscal calendar)
INSERT INTO dim_fiscal_year (fiscal_year, start_date, end_date, is_current_fy)
VALUES (2025, '2024-07-01', '2025-06-30', TRUE);

-- First, add the two_digit_fy column
ALTER TABLE dim_fiscal_year
    ADD COLUMN two_digit_fy VARCHAR(2);

-- Drop existing trigger
DROP TRIGGER IF EXISTS trigger_update_fiscal_year_timestamp ON dim_fiscal_year;

-- Create a function that handles both the dates and the two-digit representation
CREATE OR REPLACE FUNCTION handle_fiscal_year_data()
    RETURNS TRIGGER AS
$$
BEGIN
    -- Set the start and end dates based on fiscal year
    NEW.start_date := make_date(NEW.fiscal_year - 1, 7, 1);
    NEW.end_date := make_date(NEW.fiscal_year, 6, 30);

    -- Set the two-digit fiscal year representation
    NEW.two_digit_fy := RIGHT(NEW.fiscal_year::TEXT, 2);

    -- Update the timestamp
    NEW.updated_at := CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create new trigger that fires BEFORE INSERT OR UPDATE
CREATE TRIGGER trigger_handle_fiscal_year_data
    BEFORE INSERT OR UPDATE
    ON dim_fiscal_year
    FOR EACH ROW
EXECUTE FUNCTION handle_fiscal_year_data();

-- 1. Remove duplicates
-- 2. Standardize the Data
-- 3. NUll or blank values
-- 4. Remove unnecessary col, rows

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1. REMOVING DUPLICATES
WITH duplicate_cte AS
         (SELECT *,
                 ROW_NUMBER() over ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
          FROM layoffs_staging)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

WITH duplicate_cte AS
         (SELECT *,
                 ROW_NUMBER() over ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
          FROM layoffs_staging)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

create table world_layoffs.layoffs_staging2
(
    company               text null,
    location              text null,
    industry              text null,
    total_laid_off        text null,
    percentage_laid_off   text null,
    date                  text null,
    stage                 text null,
    country               text null,
    funds_raised_millions text null,
    row_num int
);

INSERT INTO layoffs_staging2
SELECT *,
       ROW_NUMBER() over ( PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standardizing data

-- Remove white spaces
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2 ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- We have Crypto, Crypto Currency & CryptoCurrency
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- REMOVE . at the end
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- FORMAT THE DATE
SELECT `date`,
       STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

SELECT *
from layoffs_staging2
where `date` = 'NULL';

UPDATE layoffs_staging2
SET `date` = NULL
where `date` = 'NULL';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
    MODIFY COLUMN `date` DATE;

-- 3. NUll or blank values

-- Populate it if Possible
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = 'NULL' OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
         JOIN layoffs_staging2 t2 on t1.company = t2.company AND t2.location = t1.location
WHERE (t1.industry IS NULL OR t1.industry = 'NULL' OR t1.industry = '')
  AND (t2.industry IS NOT NULL AND t2.industry != 'NULL' AND t2.industry != '');


UPDATE layoffs_staging2 t1
    JOIN layoffs_staging2 t2 on t1.company = t2.company AND t2.location = t1.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = 'NULL' OR t1.industry = '')
  AND (t2.industry IS NOT NULL AND t2.industry != 'NULL' AND t2.industry != '');

-- DELETE unusable ROWS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = 'NULL'
AND percentage_laid_off = 'NULL';

DELETE
FROM layoffs_staging2
WHERE total_laid_off = 'NULL'
  AND percentage_laid_off = 'NULL';

-- REMOVE unused columns
ALTER TABLE layoffs_staging2
    DROP COLUMN row_num;

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

ALTER TABLE layoffs_staging2
    MODIFY COLUMN total_laid_off INT;

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL';

ALTER TABLE layoffs_staging2
    MODIFY COLUMN percentage_laid_off DOUBLE;

UPDATE layoffs_staging2
SET funds_raised_millions = NULL
WHERE funds_raised_millions = 'NULL';

ALTER TABLE layoffs_staging2
    MODIFY COLUMN funds_raised_millions DOUBLE;
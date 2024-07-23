-- Data Cleaning
-- This data set is from Alex Freberg's Github
-- Exploratory Data Analysis (EDA) will be done in this data set.

-- data was imported from the folder
-- The data was named layoffs_n. This will be the 2nd time I will clean this data.
-- The first was guided by Alex in YT. This time, I will do it myself, without guide.

SELECT *
FROM layoffs_n;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

-- First, I created a staging or copy of the raw data set named 'layoff_staging_1_n'
-- The purpose is to safely work with a back up of raw data
CREATE TABLE layoffs_staging_1_n
LIKE layoffs_n;

SELECT *
FROM layoffs_staging_1_n;

-- Inserting the raw data to the newly created table
INSERT layoffs_staging_1_n
SELECT *
FROM layoffs_n;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging_1_n;
-- with the preceeding chunk of code, row_num column was created to check for duplicated rows, rows responded as 2

-- The next chunk of code, CTE, is created to query the duplicated rows.
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging_1_n
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging_1_n
WHERE company = 'Casper';

-- Challenge. Removings the rows numbered greater than 1.
-- For me to remove the duplicates.
-- One option is to create another table, layoffs_staging_2_n and query

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging_1_n
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;
-- THIS CODE CHUNK RETURNS an Error Code: 1288. The target table duplicate_cte of the DELETE is not updatable

CREATE TABLE `layoffs_staging2_n` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2_n
WHERE row_num > 1;

INSERT INTO layoffs_staging2_n
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY  company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging_1_n;

SELECT *
FROM layoffs_staging2_n
WHERE row_num > 1;

DELETE
FROM layoffs_staging2_n
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2_n;

-- STANDARDIZING DATA

SELECT company, TRIM(company)
FROM layoffs_staging2_n;

UPDATE layoffs_staging2_n
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2_n
ORDER BY industry;

SELECT *
FROM layoffs_staging2_n
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2_n
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2_n
ORDER BY 1;

UPDATE layoffs_staging2_n
SET country = 'United States'
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2_n
ORDER BY country;

SELECT `date`
FROM layoffs_staging2_n;

UPDATE layoffs_staging2_n
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2_n
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2_n
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2_n
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging2_n
WHERE company LIKE 'Bally%';

UPDATE layoffs_staging2_n
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry
FROM layoffs_staging2_n t1
JOIN layoffs_staging2_n t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
	AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging2_n t1
JOIN layoffs_staging2_n t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2_n;


DELETE
FROM layoffs_staging2_n
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT row_num
FROM layoffs_staging2_n;

ALTER TABLE layoffs_staging2_n
DROP COLUMN row_num;




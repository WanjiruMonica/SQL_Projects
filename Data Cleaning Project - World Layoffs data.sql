-- Data cleaning project
-- Dataset is world layoffs. Columns include the companies where layoffs took place, the location, industry, numbers of people laid off  and the countries

SELECT *
FROM layoffs;

							-- Step 1. Create a duplicate table to work on so that incase of any mistakes, the original dataset is not affected
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT*
FROM layoffs;

SELECT *
FROM layoffs_staging;

							-- Step 2. Removing Duplicates within the dataset if any

-- Using row number to identify unique and duplicate rows
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Using CTE to filter out duplicates i.e rows with row_num greater than 1
WITH CTE_duplicate AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT*
FROM CTE_duplicate
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- In mysql, there is no option to delete from the duplicate_cte the rows we do not want
-- Therefore, we create a new table that includes the column row_num from which we can delete only the duplicate rows and leave one copy
CREATE TABLE `layoffs_staging2` (
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
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

-- Deleting the duplicate rows
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


							-- Step 3. Standardizing the data
-- Trimming first column (Removing spaces)
SELECT company, TRIM(company)
FROM layoffs_staging2;   

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- Getting unque values from the column industry and making sure there are no two different names referring to the same industry   
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;                    

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Removing the period at the end of the United states in the country column
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Changing the date column from text to an actual date
SELECT `date`,
STR_TO_DATE(`date` ,'%m/%d/%Y')
FROM layoffs_staging2;

-- This step changes how the date is formatted in the column but does not change the fact that the data is stored as text
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date` ,'%m/%d/%Y');

-- This step changes the way the data is stored from text to date. To confirm it has worked, selct the column name on the schemas panel and check below if it says date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;



							-- Step 3. Dealing with Null and Blanks
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- This is a self join . To understand which rows are of the same company while the industry of one or more rows is blank
SELECT t1.company, t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2. company      -- Joining is on company because the company column is fully populated but the industry column is not fully populated
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Set the blank columns to null to easily populate
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Update the null columns with the right industry
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2. company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Remove rows where total_laid_off and percentage_laid_off is NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Removing the column for row number because it is not necessary in the analysis
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;














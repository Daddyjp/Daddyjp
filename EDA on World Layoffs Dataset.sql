-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2_n;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2_n;

SELECT *
FROM layoffs_staging2_n
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2_n;


SELECT country, SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY country
ORDER BY 2 DESC;

SELECT *
FROM layoffs_staging2_n;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY stage
ORDER BY 2 DESC;


SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2_n
GROUP BY company
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off)
FROM layoffs_staging2_n
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1
;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `month`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2_n
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)
SELECT `month`, 
total_off,
SUM(total_off) OVER(ORDER BY `month`)
FROM Rolling_Total;



SELECT company, SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2_n
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;












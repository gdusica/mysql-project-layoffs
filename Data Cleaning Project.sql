-- Data Cleaning Project (data: world_layoffs.csv)

select *
from layoffs;

-- 1. remove duplicates
-- 2. standardize the data
-- 3. null/blank values
-- 4. remove any columns that are unnecessary

-- pravljenje kopije raw tabele za svaki slucaj
create table layoffs_staging
like layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;

select *
from layoffs_staging;

-- 1. REMOVE DUPLICATES
select * ,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

-- create a CTE or subquery to extract companies along with row_num (number of duplicates)
with duplicate_cte as
(select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;

-- one of the companies found to have duplicates
select *
from layoffs_staging
where company = 'Casper';
-- we don't want to delete both duplicates, just one, so we don't use delete from l_s where company='...'
-- repeat the CTE, but this time the query is a delete instead of a select
with duplicate_cte as
(select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;
-- however, this is not possible because CTE cannot be used directly for update or delete

-- instead, we'll create a new table (right-click on layoffs_staging -> Copy to Clipboard ->
-- create statement) that looks like layoffs_staging but we'll add a row_num column

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
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- then we'll add data to it (using the query from above)
insert into layoffs_staging2
select * ,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;
-- check if rows with duplicates have been deleted
select *
from layoffs_staging2
where row_num > 1;

-- 2. STANDARDIZING DATA

-- COLUMN company
-- see if there are blank spaces in company names
select company, trim(company)
from layoffs_staging2;
-- removing blank spaces from column 'company'
update layoffs_staging2
set company = trim(company);

-- COLUMN industry
select distinct(industry)
from layoffs_staging2
order by 1;
select *
from layoffs_staging2
where industry like '%Crypto%';
-- there are industries - Crypto, Crypto Currency, CryptoCurrency - which are the same
-- 90+% of companies into these industries are 'Crypto' so we are going to name all them after that
update layoffs_staging2 
set industry = 'Crypto' 
where industry like 'Crypto%';

-- again look at result from this query
select distinct(industry)
from layoffs_staging2
order by 1;
-- there are nulls and empty strings that are issues
-- we'll handle this later

-- COLUMN location
select distinct location
from layoffs_staging2
order by 1;
-- this looks pretty good

-- COLUMN country
select distinct country
from layoffs_staging2
order by 1;
-- United States / United States.
select *
from layoffs_staging2
where country = "United States";
-- 1000 vs 4 rows
select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;
-- 1st way
update layoffs_staging2
set country='United States' 
where country='United States.';
-- 2nd way
update layoffs_staging2
set country=trim(trailing '.' from country)
where country LIKE '%.';

-- COLUMN date
-- change type from text to date
select `date`,
str_to_date(`date`, '%m/%d/%Y') as datedate
from layoffs_staging2;
-- set values of column date in the date format m/d/Y
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');
-- change type from text do date
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
-- check if everything is alright
select `date`
from layoffs_staging2;

-- 3. Dealing with null and blank values + 4. Deleting unnecessary rows/columns

-- COLUMN total_laid_off
select *
from layoffs_staging2
where total_laid_off is null;
-- mora is null, ne moze ==null
-- 739 rows returned
-- rows where total_laid_off and percentage_laid_off are both null can be useless
select *
from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null;
-- 361 rows returned
-- so we are going to remove these because I didn't find purpose of these rows without values in each of these columns
delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null or industry='';

select *
from layoffs_staging2
where company = 'Airbnb';
-- there are companies like Airbnb that shows multiple times in rows, in some cases they have 
-- null/blank industry and in the others real industry
select *
from layoffs_staging2 t1 
join layoffs_staging2 t2
on t1.company=t2.company and t1.location=t2.location
where (t1.industry is null or t1.industry= '') and t2.industry is not null;
-- 7 rows returned

-- changing from blank to null
update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;

select *
from layoffs_staging2
where industry is null or industry='';
-- we can conclude that company Bally's Interactive doesn't have a pair that has non-null value for column 'industry'

-- FINALLY
select *
from layoffs_staging2;
-- 1995 row returned

-- drop a column row_num so thath we can transfer the data from this table to the initial one
alter table layoffs_staging2
drop column row_num;


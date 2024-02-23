-- Databricks notebook source
-- MAGIC %md
-- MAGIC This is a databricks notebook, where I already uploaded a few datasets as csv files, to create tables.
-- MAGIC This notebook is just to show my SQL skills in regards to queries, set theory etc.
-- MAGIC
-- MAGIC I'm using this data set: https://www.kaggle.com/datasets/holmjason2/videogamedata, uploaded as a table in databricks

-- COMMAND ----------

-- 1)
-- first let's just get a brief overview of the dataset
SELECT *
FROM games;


-- COMMAND ----------

-- 2)
-- what period are we working with?
SELECT MIN(Year), MAX(Year)
FROM games;

-- okay, so games released from 1977-2020

-- COMMAND ----------

-- How many rows are there?
SELECT COUNT(*)
FROM games;

-- So 19600 entries!

-- COMMAND ----------

-- lets just look at some other min and max values from the dataset
SELECT MIN(Critic_Score), MAX(Critic_Score), MIN(User_Score), MAX(User_Score), MIN(Total_Shipped), MAX(Total_Shipped)
FROM games
WHERE Critic_Score IS NOT NULL AND User_Score IS NOT NULL;

-- COMMAND ----------

-- lets look at medians
SELECT median(User_Score), median(Critic_Score), median(Total_Shipped)
FROM games
WHERE Critic_Score IS NOT NULL AND User_Score IS NOT NULL;

-- COMMAND ----------

-- let's rename the column total_shipped to games_sold (let's just assume that is roughly the same amount)
--  okay so that's not supported and I don't know how to fix it, but I'm just gonna leave it like this for now. Then I can show how I fix it, when I figure it out.

ALTER TABLE games
RENAME COLUMN Total_Shipped TO games_sold;

-- COMMAND ----------

-- 3)
--Select all columns for the top ten best-selling video games (based on Total_Shipped) in games.
--Order the results from the best-selling game down to the tenth best-selling game.
SELECT *
FROM games
ORDER BY Total_Shipped DESC -- order by highest-lowest
LIMIT(10); -- get the first 10 rows

-- COMMAND ----------

-- 4)
-- Let's determine how many games in the games table are missing both a user_score and a critic_score.
-- Select the count of games where both the associated critic_score and the associated user_score are null.

SELECT COUNT(*)
from games
WHERE Critic_Score IS NULL
AND User_Score IS NULL;

-- so 9616 of the games does not have a critic score and user score



-- COMMAND ----------

-- 5)
-- I would like to remove the rows without critics and user scores, but like before, altering tables does not seem to be allowed using the current catalog implementation.
-- I still don't know how to fix it, but I'm just gonna leave it like this for now. Then I can show how I fix it, when I figure it out.
-- Until then I can just remove NULL's when querying
DELETE FROM games WHERE User_Score IS NULL AND Critic_Score IS NULL;

-- COMMAND ----------

-- 6)
-- Select year and average critic score for each year. The average critic score for each year is rounded to two decimals and aliased as avg_critic_score.
-- Group the data by release year.
-- Order the data from highest to lowest avg_critic_score and limit the results to the top ten years.
-- Remove nulls
-- let's just create a view since I might want to use the result later.

CREATE VIEW top_critic_years AS
SELECT Year, ROUND(AVG(Critic_Score),2) AS avg_critic_score 
FROM games
WHERE User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Year
ORDER BY avg_critic_score DESC
LIMIT(10);



-- COMMAND ----------

-- and let's just look at the created view

SELECT *
FROM top_critic_years

-- COMMAND ----------

-- 7)
-- Find game critics' ten favorite years, this time with the constraint that a year must have more than four games released in order to be considered.
-- Update the query above to include a count of games released in a given year, aliased as num_games.
-- The query is filtered so that only years with more than 25 games released are returned.
-- and let's make a view, since I want to use the result again in a bit

CREATE VIEW top_critic_years_more_than_25_games AS
SELECT Year, COUNT(Name) AS num_games, ROUND(AVG(Critic_Score),2) AS avg_critic_score 
FROM games
WHERE User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Year
HAVING COUNT(Name) > 25
ORDER BY avg_critic_score DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM top_critic_years_more_than_25_games;

-- COMMAND ----------

-- 8)
-- Find the year and avg_critic_score for those years that were on our the first critics' favorite list but not the second due to having 10 or fewer reviewed games.
-- Order the results from highest to lowest avg_critic_score.

SELECT Year, avg_critic_score
FROM top_critic_years
EXCEPT
SELECT Year, avg_critic_score
FROM top_critic_years_more_than_25_games
ORDER BY avg_critic_score DESC;

-- COMMAND ----------

-- 9)
-- Update the query from task 7 so that it returns years with ten highest avg_user_score values.
-- Select year and an average of user_score for each year, rounded to two decimal places and alias as avg_user_score
-- Include a count of games released in a given year, aliased as num_games.
-- Include only years with more than 25 reviewed games
-- group the data by year.
-- Order data from highest to lowest avg_user_score, and limit the results to the top ten years.

CREATE VIEW top_user_years_more_than_25_games AS
SELECT Year, COUNT(Name) AS num_games, ROUND(AVG(User_Score),2) AS avg_user_score 
FROM games
WHERE User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Year
HAVING COUNT(Name) > 25
ORDER BY avg_user_score DESC
LIMIT 10;

-- COMMAND ----------

SELECT *
FROM top_critic_years_more_than_25_games;

-- COMMAND ----------

-- 10)
-- Find years that appear on both the top_critic_years_more_than_25_games table and the top_user_years_more_than_25_games table.
-- Use set theory (intersection), to find only the year results that appear on both tables.

SELECT Year
FROM top_critic_years_more_than_25_games
INTERSECT
SELECT Year
FROM top_user_years_more_than_25_games;


-- COMMAND ----------

-- Make a column showing total games_sold in each year to the table from above.
-- Select year and the sum of games_sold, aliased as total_games_sold; order the results by total_games_sold descending.
-- Filter the game_sales table based on whether the year is in the list of years in the table above, using the code from the previous task as a subquery.
-- Group the results by year.

SELECT Year, SUM(Total_Shipped) AS total_games_sold
FROM games
WHERE Year IN (SELECT Year 
              FROM top_user_years_more_than_25_games
              INTERSECT
              SELECT Year
              FROM top_critic_years_more_than_25_games) AND User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Year
ORDER BY total_games_sold DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Looking at the last query above, it seems like the top 3 for best years in videogames are:
-- MAGIC 2017, 2014 and 2015.

-- COMMAND ----------

-- Since I'm not THAT into videogames, I don't know why these years are great, so let's look at what videogames where released those years, ordered by the most sold (Total shipped)?

SELECT Year, Name,Total_Shipped
FROM games
WHERE Year IN (2017,2014,2015) AND User_Score IS NOT NULL AND Critic_Score IS NOT NULL
ORDER BY Total_Shipped DESC;

-- COMMAND ----------

-- And which publishers released most games those years?
SELECT Publisher
FROM games
WHERE Year IN (2017,2014,2015) AND User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Publisher
ORDER BY COUNT(Publisher) DESC
LIMIT(10);

-- COMMAND ----------

-- How about looking at the worst games those years?
SELECT Year, Name, Total_Shipped
FROM games
WHERE Year IN (2017,2014,2015) AND User_Score IS NOT NULL AND Critic_Score IS NOT NULL
ORDER BY Total_Shipped ASC
LIMIT(20);


-- COMMAND ----------

-- And what platforms where people using for all of this gaming?

SELECT Platform
FROM games
WHERE Year IN (2017,2014,2015) AND User_Score IS NOT NULL AND Critic_Score IS NOT NULL
GROUP BY Platform
ORDER BY COUNT(Platform) DESC;

-- Okay so PS4 is the most popular choice

-- COMMAND ----------

-- How about finding the best selling game for each year?
SELECT Year, MAX(User_Score) AS max_score
FROM games
WHERE Critic_Score IS NOT NULL AND User_Score IS NOT NULL
GROUP BY Year
ORDER BY Year DESC;

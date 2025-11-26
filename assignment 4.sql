-- RAW LAYER
-- 1.1
CREATE OR REPLACE TABLE raw_games_source AS 
SELECT * FROM read_json_auto(
    'https://github.com/vintagedon/steam-dataset-2025/raw/main/data/01_raw/steam_2025_5k-dataset-games_20250831.json.gz',
    maximum_object_size=268435456
);

-- 2. Load Raw Reviews Data from GitHub
CREATE OR REPLACE TABLE raw_reviews_source AS 
SELECT * FROM read_json_auto(
    'https://github.com/vintagedon/steam-dataset-2025/raw/main/data/01_raw/steam_2025_5k-dataset-reviews_20250901.json.gz', 
    maximum_object_size=268435456
);

-- Based on the inspection of the raw JSON files, here are the columns containing semi-structured data (arrays, nested objects, or structs) that require parsing or unnesting.
-- Games Dataset (steam_2025_5k-dataset-games_20250831.json.gz)
-- The root object contains a list games. Inside each game object, the app_details.data structure is highly nested.
-- app_details.data.price_overview (Object): Contains currency, initial, final, etc.
-- app_details.data.release_date (Object): Contains date and coming_soon boolean.
-- app_details.data.developers (Array): List of developer names (Strings).
-- app_details.data.publishers (Array): List of publisher names (Strings).
-- app_details.data.genres (Array of Objects): Contains id and description for each genre.
-- app_details.data.categories (Array of Objects): Contains id and description for game tags/categories.
-- app_details.data.platforms (Object): Boolean flags for windows, mac, linux.
-- app_details.data.screenshots & movies (Arrays of Objects): Media assets metadata.
-- app_details.data.pc_requirements (Object): Hardware specs.

-- Reviews Dataset (steam_2025_5k-dataset-reviews_20250901.json.gz)
--The file consists of a root reviews list. Each item represents a game, which contains a nested list of actual user reviews.
-- review_data (Object): Container for summary and review list.
-- review_data.query_summary (Object): Aggregate stats like total_positive, total_reviews.
-- review_data.reviews (Array of Objects): The actual list of user reviews. This requires UNNEST.
-- review_data.reviews[].author (Object): Nested inside the review array, contains steamid, playtime_forever, etc.

-- STAGE LAYER
-- 1.3 - 1.5
CREATE OR REPLACE TABLE games_clean AS
SELECT
g.appid,
g.app_details.data.name AS game_name,
COALESCE(g.app_details.data.price_overview.final / 100.0, 0) AS price,
g.app_details.data.price_overview.currency AS currency,
g.app_details.data.release_date.date AS release_date,
g.app_details.data.genres AS genres,
g.app_details.data.developers AS developers,
g.app_details.data.categories AS categories
FROM (
-- Unnest from the raw source table we just created
SELECT unnest(games) as g
FROM raw_games_source
);

CREATE OR REPLACE TABLE reviews_clean AS
SELECT
r.appid,
review_item.recommendationid AS review_id,
review_item.author.steamid AS author_steamid,
review_item.voted_up AS is_positive,
review_item.votes_up AS votes_helpful,
    TRY_CAST(review_item.weighted_vote_score AS DOUBLE) AS vote_score,
    review_item.review AS review_text,
    to_timestamp(review_item.timestamp_created) AS created_at
FROM (
SELECT unnest(reviews) as r
FROM raw_reviews_source
),
-- 2. Unnest the internal list.
-- FIX: We alias the table as 't' and the column as 'review_item'
UNNEST(r.review_data.reviews) AS t(review_item);

-- MART LAYER
-- 2.1
SELECT
g.game_name,
COUNT(r.review_id) AS review_count
FROM reviews_clean r
JOIN games_clean g ON r.appid = g.appid
GROUP BY g.game_name
ORDER BY review_count DESC
LIMIT 20;
-- The top games in this sample dataset (such as Command & Conquer Red Alert 2, Rust, and The Persistence) all appear to be capped at 100 reviews each.
-- This indicates the dataset likely limits the number of reviews harvested per game to a maximum of 100.

-- 2.2
SELECT
RIGHT(release_date, 4) AS release_year,
COUNT(*) AS game_count
FROM games_clean
WHERE release_date IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;
-- The dataset is heavily skewed towards recent and future titles, with 2024 (1109 games) and 2025 (1175 games) being the most represented years.
-- There are significantly fewer games from older years (e.g., only 117 from 2014), suggesting the dataset focuses on new or upcoming releases.

-- 2.3
SELECT
genre.description AS genre_name,
AVG(price) AS avg_price
FROM games_clean,
UNNEST(genres) AS t(genre)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
-- The pricing varies significantly by category.
-- Specialized software categories like "Animation & Modeling" ($36.84) and "Design & Illustration" ($21.36)
-- have higher average prices compared to traditional game genres like "Adventure" ($16.00) or "Simulation" ($14.77).

-- 2.4
SELECT
category.description AS tag_name,
COUNT(*) AS tag_count
FROM games_clean,
UNNEST(categories) AS t(category)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
-- "Single-player" is by far the most dominant feature, present in 6,692 records.
-- "Family Sharing" (5,208) and "Steam Achievements" (3,148) are also extremely common,
-- showing that social sharing and gamification features are standard in the modern Steam library.

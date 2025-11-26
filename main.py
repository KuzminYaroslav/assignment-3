import duckdb
import matplotlib.pyplot as plt
import pandas as pd

# 1. Connect to DuckDB (in-memory)
con = duckdb.connect()

print("Loading data from GitHub... this may take a moment.")

# 2. SETUP: Load Raw Data & Create 'games_clean' Table
# We execute the same SQL logic from Part 1 here to prepare the database
con.execute("""
    -- Load Raw Games Data with increased buffer for large JSON object
    CREATE OR REPLACE TABLE raw_games_source AS 
    SELECT * FROM read_json_auto(
        'https://github.com/vintagedon/steam-dataset-2025/raw/main/data/01_raw/steam_2025_5k-dataset-games_20250831.json.gz',
        maximum_object_size=268435456
    );

    -- Create Clean Games Table (Parsing & Unnesting)
    CREATE OR REPLACE TABLE games_clean AS
    SELECT 
        g.appid,
        g.app_details.data.name AS game_name,
        -- Convert price: cents -> dollars, handle NULLs
        COALESCE(g.app_details.data.price_overview.final / 100.0, 0) AS price,
        g.app_details.data.genres AS genres
    FROM (
        SELECT unnest(games) as g 
        FROM raw_games_source
    );
""")

print("Data loaded successfully. Generating chart...")

# 3. ANALYZE: Run the Analytical Query
query = """
SELECT 
    genre.description AS genre_name,
    ROUND(AVG(price), 2) AS avg_price
FROM games_clean,
UNNEST(genres) AS t(genre)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
"""

# 4. VISUALIZE: Create the Chart
df = con.sql(query).df()

plt.figure(figsize=(12, 6))
# Create horizontal bar chart
bars = plt.barh(df['genre_name'], df['avg_price'], color='#2a475e')

# Styling
plt.xlabel('Average Price ($)', fontsize=12)
plt.title('Top 10 Genres by Average Price (Steam 2025 Dataset)', fontsize=14, pad=20)
plt.gca().invert_yaxis()  # Sort highest price at top
plt.grid(axis='x', linestyle='--', alpha=0.7)

# Add price labels
for bar in bars:
    width = bar.get_width()
    plt.text(width + 0.5, bar.get_y() + bar.get_height()/2,
             f'${width}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('genre_pricing_chart.png')
print("Chart saved to 'genre_pricing_chart.png'")
plt.show()

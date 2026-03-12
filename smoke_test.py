# -*- coding: utf-8 -*-
import sys, sqlite3, ast
import pandas as pd

# Syntax check app.py
with open('C:/Users/brady/ipeds_completions/app.py', encoding='utf-8') as f:
    src = f.read()
ast.parse(src)
print('Syntax OK')

conn = sqlite3.connect('C:/Users/brady/ipeds_completions/ipeds.db')
conn.execute("CREATE INDEX IF NOT EXISTS idx_inst_cbsa ON institutions(cbsa)")
conn.commit()

# National CS bachelors trend
df = pd.read_sql_query("""
    SELECT year, SUM(ctotalt) AS completions
    FROM completions_view
    WHERE cipcode LIKE '11.%' AND awlevel=5 AND majornum=1
      AND (closeind IS NULL OR closeind=0) AND ctotalt>0
    GROUP BY year ORDER BY year
""", conn)
print("National CS Bachelors:")
print(df.to_string(index=False))

# State filter
df2 = pd.read_sql_query("""
    SELECT year, SUM(ctotalt) AS completions
    FROM completions_view
    WHERE cipcode LIKE '51.%' AND awlevel IN (3,5,7)
      AND stabbr IN ('CA','TX') AND majornum=1
      AND (closeind IS NULL OR closeind=0) AND ctotalt>0
    GROUP BY year ORDER BY year
""", conn)
print("\nHealth CA+TX (Assoc/Bach/Masters):")
print(df2.to_string(index=False))

# Metro check
n = conn.execute("SELECT COUNT(DISTINCT cbsa) FROM institutions WHERE cbsa!=''").fetchone()[0]
print(f"\nDistinct CBSAs in DB: {n}")

# Sample metros
metros = conn.execute("""
    SELECT cbsa, cbsanm, COUNT(DISTINCT unitid) as insts
    FROM institutions WHERE cbsa!='' AND cbsanm!=''
    GROUP BY cbsa, cbsanm ORDER BY insts DESC LIMIT 5
""").fetchall()
print("Top metros by institution count:")
for r in metros:
    print(f"  {r[1]} ({r[0]}): {r[2]} institutions")

conn.close()
print("\nAll checks passed.")

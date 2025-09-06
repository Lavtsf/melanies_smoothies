# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd
import math

st.title("🥤 Customize Your Smoothie 🥤")
st.write("Choose the fruits you want in your custom smoothie")

# Name input
name_on_order = st.text_input("Name on Smoothie", "Life of Brian")
st.write("The name on your smoothie will be:", name_on_order)

# Snowflake connection
cnx = st.connection("snowflake")
session = cnx.session()

# Fetch fruit options from Snowflake
snow_df = session.table("smoothies.public.fruit_options").select(
    col("FRUIT_NAME"), col("SEARCH_ON")
)

# Convert Snowpark DataFrame to Pandas and index by FRUIT_NAME for fast lookup
pd_df = snow_df.to_pandas().drop_duplicates(subset=["FRUIT_NAME"])
pd_df = pd_df.set_index("FRUIT_NAME", drop=False)

# Multiselect on fruit names
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    options=pd_df["FRUIT_NAME"].tolist(),
    max_selections=5
)

if ingredients_list:
    # Build a nice comma-separated list for storage
    ingredients_string = ", ".join(ingredients_list)

    for fruit_chosen in ingredients_list:
        # Ensure the chosen fruit exists in the dataframe
        if fruit_chosen not in pd_df.index:
            st.warning(f"'{fruit_chosen}' not found in options. Skipping.")
            continue

        # Get search key and CAST TO STRING safely
        search_on_value = pd_df.at[fruit_chosen, "SEARCH_ON"]

        # Guard against NaN/None
        if search_on_value is None or (isinstance(search_on_value, float) and math.isnan(search_on_value)):
            st.warning(f"No SEARCH_ON value for '{fruit_chosen}'. Skipping nutrition lookup.")
            continue

        search_on = str(search_on_value).strip()
        if not search_on:
            st.warning(f"Empty SEARCH_ON for '{fruit_chosen}'. Skipping.")
            continue

        # Show nutrition info
        st.subheader(f"{fruit_chosen} — Nutrition Information")
        url = f"https://my.smoothiefroot.com/api/fruit/{search_on}"

        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # Normalize JSON to a dataframe for display
            if isinstance(data, dict):
                df = pd.json_normalize(data)
            else:
                df = pd.DataFrame(data)

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Failed to fetch nutrition for {fruit_chosen}: {e}")

    # Prepare and execute insert (escape single quotes)
    name_sql = name_on_order.replace("'", "''")
    ingredients_sql = ingredients_string.replace("'", "''")

    insert_sql = f"""
        INSERT INTO smoothies.public.orders (ingredients, NAME_ON_ORDER)
        VALUES ('{ingredients_sql}', '{name_sql}')
    """

    st.code(insert_sql, language="sql")
    if st.button("Submit Order"):
        try:
            session.sql(insert_sql).collect()
            st.success("Your Smoothie is ordered!", icon="✅")
        except Exception as e:
            st.error(f"Insert failed: {e}")

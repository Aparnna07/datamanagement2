import pandas as pd


file_path = r"C:\Users\HP\Downloads\madrid.csv"
df = pd.read_csv(file_path)


columns_to_keep = [
    'id',
    'host_name',
    'latitude',
    'longitude',
    'price',
    'availability_30',
    'number_of_reviews',
    'minimum_nights',
    'last_scraped',
    'room_type'
]


df_cleaned = df[columns_to_keep].copy()


df_cleaned.columns = [col.strip().lower().replace(" ", "_") for col in df_cleaned.columns]


df_cleaned['last_scraped'] = pd.to_datetime(df_cleaned['last_scraped'], errors='coerce')


df_cleaned['city'] = 'Madrid'


df_cleaned['listing_date'] = df_cleaned['last_scraped'].dt.date
df_cleaned['listing_time'] = df_cleaned['last_scraped'].dt.time


df_cleaned.to_csv('Madrid_cleaned.csv', index=False)

print(" Cleaned data saved as 'Madrid_cleaned.csv'. Columns now are:")
print(list(df_cleaned.columns))

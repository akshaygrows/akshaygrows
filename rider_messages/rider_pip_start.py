from rider_messages.postgres_connect import *

# Getting new data and adding in the DB
path = '/Users/apple/Documents/tunnel-ssh.cer'
conn = get_conn('Yes',path)

query_path = 'new_week_rider_pip.sql'
with open(query_path, 'r') as file:
    custom_query = file.read()
df_pip = pd.read_sql(custom_query,conn)


from io import StringIO
csv_data = df_pip.to_csv(index=False, header=False, sep='\t')

cursor = conn.cursor()

# Copy the CSV data into the main table using COPY FROM
copy_data_query = f"""
COPY rider_pip FROM STDIN WITH CSV DELIMITER '\t' HEADER;
"""
cursor.copy_expert(copy_data_query, StringIO(csv_data))
conn.commit()

cursor.close()

#initiating messages on riders added
query = '''
select 
	pip.rider_id
,	pip.rider_name
,	pip.need_improvement
,	shipping_city
,	r.phone
from rider_pip  as pip
left join application_db.rider as r on pip.rider_id = r.locus_rider_id
where pip_date = date_trunc('week',now()+interval '5.5 hours')
'''

df_riders = pd.read_sql(query,conn)
df_riders.head()

import requests

for index, row in df_riders.iterrows():
    url = "https://api.fyno.io/v1/FYAP451610107IN/test/event"

    # Determine the event based on the 'shipping_city' column
    if row['shipping_city'] in ['Jaipur', 'Delhi', 'Mumbai']:
        event = "rider_pip_start_hindi"
    else:
        event = "rider_pip_start_english"

    # Add "91" before the phone number
    phone_with_country_code = "91" + str(row['phone'])

    payload = {
        "to": {"whatsapp": phone_with_country_code},
        "data": {
            "rider_name": row['rider_name'],
            "need_improvement": row['need_improvement'],
            "newKey": "New Value"
        },
        "event": event
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": "Basic RllBUDQ1MTYxMDEwN0lOOnRYVlRkSEkuK3pzdTQyVG80elBGKzNvZHdZMDFPdjdMdE9oSkpqUXg="
    }

    response = requests.post(url, json=payload, headers=headers)

    print(f"Row {index + 1} - API Response: {response.text}")
print("All steps done exiting")

conn.close()



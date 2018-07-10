import requests
import os.path
import psycopg2

from config import config

def fetch_data():
    location_list = ['London', 'Santorini', 'Santiago', 'San Francisco', 'Mexico City', 'Hong Kong']
    location_id_list = []

    print('querying location ids')
    # query location id with location name
    for location in location_list:
        query_id_url = 'https://www.metaweather.com/api/location/search/?query=%s' %(i)
        ids = requests.get(query_id_url).json()
        location_id_list.append(ids[0]['woeid'])
    print('location ids retrieved')
    
    # record ids and apt_long in a .txt file
    write_into('Users/zihuizheng/Desktop/weather_analysis/location_id_log.txt', ids)

    # request data with location id
    print('start fetching data')
    for id in location_id_list:
        url = 'https://www.metaweather.com/api/location/%s' % (id)
        data = requests.get(url).json()
        weather = data['consolidated_weather']
        create_date = weather[0]['created'][:10]
        applicable_date = [record['applicable_date'] for record in weather]
        weather_predict = [record['weather_state_name'] for record in weather]
        min_temperature = [str(record['min_temp'])[:4] for record in weather]
        max_temperature = [str(record['max_temp'])[:4] for record in weather]
        air_pressure = [str(record['air_pressure'])[:7] for record in weather]
        humidity = [str(record['humidity']) for record in weather]
    print('fetching data success')
    return create_date, applicable_date, weather_predict, min_temperature, max_temperature, air_pressure, humidity


def write_into(path, content):
    if os.path.isfile(path):
        with open(path, 'a+') as fh:
            fh.write(content)
    else:
        with open(path, 'w+') as fh:
            fh.write(content)
    print('location ids and apt_long info recorded in location_id_log.txt')


def create_table():
    commands = ('''
                create table weather_analysis if not exists(
                    ids integer serial unique,
                    create_date date,
                    applicable_date date,
                    weather_predict varchar(30),
                    min_temperature float(3)
                    max_temperature float(3),
                    air_pressure float(3),
                    humidity float(3)
                )''')
    conn = None
    try:

        params = config()

        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        cur.execute(commands)

        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_data():
    conn = None
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    create_date, applicable_date, weather_predit, min_temperature, max_temperature, air_pressure, humidity = fetch_data()
    while create_date:
        create_d = create_date.pop()
        applicable_d = applicable_date.pop()
        weather_p = weather_predit.pop()
        min_temp = min_temperature.pop()
        max_temp = max_temperature.pop()
        air_p = air_pressure.pop()
        humid = humidity.pop()

        sql = '''INSERT INTO weather_analysis (create_date, applicable_date, weather_predict, min_temperature, max_temperature, air_pressure, humidity) 
        values (%, %, %, %, %, %, %);'''

        cur.execute(sql, (create_d, applicable_d, weather_p, min_temp, max_temp, air_p, humid))
        conn.commit()
    cur.close()

if __name__ == '__main__':
    create_table()
    insert_data()



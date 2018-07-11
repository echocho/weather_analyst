import requests
import os.path
import psycopg2

from config import config

def fetch_data():
    location_list = ['Tokyo', 'London', 'Paris', 'San Diego', 'San Francisco', 'Mexico City', 'Hong Kong', 'Los Angeles']
    location_id_list = []

    print('Retrieving data from Yahoo!...')
    # query location id with location name
    for city in location_list:
        search_text = "select item.condition from weather.forecast where woeid in (select woeid from geo.places(1) where text='" + city + "') and u='c'"
        url = "https://query.yahooapis.com/v1/public/yql?q=" + search_text + "&format=json"
        data = requests.get(url).json()
    print('Success!')
    
    # record ids and apt_long in a .txt file
    write_into('Users/zihuizheng/Desktop/weather_analysis/yahoo_weather_retrieved.json', data)

    # extracting data needed
    print('Extracting data...')
    create_date = data['query']['created']
    humidity = data['query']['results']['channel']['atmosphere']['humidity']
    air_pressure = data['query']['results']['channel']['atmosphere']['pressure']
    sun_rise = data['query']['results']['channel']['astronomy']['sunrise']
    sun_set = data['query']['results']['channel']['astronomy']['sunset']
    weather_txt = data['query']['results']['channel']['item']['text']
    min_temp = data['query']['results']['channel']['item']['forecast'][0]['low']
    max_temp = data['query']['results']['channel']['item']['forecast'][0]['high']

    print('Success!')
    return create_date, humidity, air_pressure, sun_rise, sun_set, weather_txt, min_temp, max_temp


def write_into(path, content):
    if os.path.isfile(path):
        with open(path, 'a+') as fh:
            fh.write(content)
            print("Updated 'yahoo_weather_retrieved.json'")
    else:
        with open(path, 'w+') as fh:
            fh.write(content)
            print("Created 'yahoo_weather_retrieved.json'")


def create_table():
    # first check if table already exist. if no, create one; if yes, update it
    commands = ('''
                create table if not exists weather_analysis(
                    id serial,
                    create_date date,
                    humudity float(2),
                    air_pressure float(3),
                    sun_rise varchar(20),
                    sun_set varchar(20),
                    weather_txt varchar(20),
                    low_temp float(2),
                    high_temp float(2)
                );''')
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

    create_date, humidity, air_pressure, sun_rise, sun_set, weather_txt, min_temp, max_temp = fetch_data()
    while create_date:
        create_date = create_date.pop(0)
        humidity = humidity.pop(0)
        air_pressure = air_pressure.pop(0)
        sun_rise = sun_rise.pop(0)
        sun_set = sun_set.pop(0)
        weather_txt = weather_txt.pop(0)
        min_temp = min_temp.pop(0)
        max_temp = max_temp.pop(0)

        sql = '''INSERT INTO weather_analysis (id, create_date, humidity, air_pressure, sun_rise, sun_set, weather_text, low_temp, high_temp) 
        values (1, %, %, %, %, %, %, %, %);'''

        cur.execute(sql, (create_date, humidity, air_pressure, sun_rise, sun_set, weather_txt, min_temp, max_temp))
        conn.commit()
    cur.close()


if __name__ == '__main__':
    create_table()
    insert_data()



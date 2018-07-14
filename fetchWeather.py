import requests
import os.path
import psycopg2

from config import config

def fetch_data(city):
    print('Retrieving data from Yahoo!...')

    search_text = "select * from weather.forecast where woeid in (select woeid from geo.places(1) where text='" + city + "') and u='c'"
    url = "https://query.yahooapis.com/v1/public/yql?q=" + search_text + "&format=json"
    data = requests.get(url).json()
    print('Success!')
    
    # record ids and apt_long in a .txt file
    write_into('yahoo_weather_retrieved.json', data)

    # extracting data needed
    print('Extracting data...')

    create_date = [data['query']['created']]
    humidity = [data['query']['results']['channel']['atmosphere']['humidity']]
    air_pressure = [data['query']['results']['channel']['atmosphere']['pressure']]
    sun_rise = [data['query']['results']['channel']['astronomy']['sunrise']]
    sun_set = [data['query']['results']['channel']['astronomy']['sunset']]
    weather_txt = [data['query']['results']['channel']['item']['condition']['text']]
    min_temp = [data['query']['results']['channel']['item']['forecast'][0]['low']]
    max_temp = [data['query']['results']['channel']['item']['forecast'][0]['high']]

    print('Success!')
    return create_date, humidity, air_pressure, sun_rise, sun_set, weather_txt, min_temp, max_temp


def write_into(path, content):
    if os.path.exists(path):
        with open(path, 'a+') as fh:
            fh.write(str(content))
            print("Updated 'yahoo_weather_retrieved.json'")
    else:
        with open(path, 'w+') as fh:
            fh.write(str(content))
            print("Created 'yahoo_weather_retrieved.json'")


def create_table():
    # first check if table already exist. if no, create one; if yes, update it
    commands = ('''
                create table if not exists weather_analysis(
                    id serial,
                    city varchar(15),
                    create_date varchar(40),
                    humidity integer,
                    air_pressure float(2),
                    sun_rise varchar(20),
                    sun_set varchar(20),
                    weather_text varchar(20),
                    low_temp integer,
                    high_temp integer
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


def insert_data(location_list):
    conn = None
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    for place in location_list:
        create_date, humidity, air_pressure, sun_rise, sun_set, weather_text, min_temp, max_temp = fetch_data(place)

        while create_date:
            create_d = create_date.pop(0)
            humid = humidity.pop(0)
            pressure = air_pressure.pop(0)
            sunrise = sun_rise.pop(0)
            sunset = sun_set.pop(0)
            weather = weather_text.pop(0)
            mintemp = min_temp.pop(0)
            maxtemp = max_temp.pop(0)

            sql = '''INSERT INTO weather_analysis (city, create_date, humidity, air_pressure, sun_rise, sun_set, weather_text, low_temp, high_temp) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s);'''

            cur.execute(sql, (place, create_d, humid, pressure, sunrise, sunset, weather, mintemp, maxtemp))
            conn.commit()
    cur.close()


if __name__ == '__main__':
    location_list = ['Tokyo', 'London', 'Paris', 'San Diego', 'San Francisco', 'Mexico City', 'Shanghai', 'Los Angeles']
    create_table()
    insert_data(location_list)



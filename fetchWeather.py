import requests

def fetchWeather():
    print('start fetching data')

    url = 'https://www.metaweather.com/api/location/2151330/'
    data = requests.get(url).json()
    weather = data['consolidated_weather']

    create_date = weather[0]['created'][:10]
    applicable_date = [record['applicable_date'] for record in weather]
    weather_predit = [record['weather_state_name'] for record in weather]
    min_temperature = [str(record['min_temp'])[:4] for record in weather]
    max_temperature = [str(record['max_temp'])[:4] for record in weather]

    full_script = ''

    for i in range(len(applicable_date)):
        full_script += applicable_date[i] + ' ' + weather_predit[i] + '  ' + min_temperature[i] + '(Low)' + ' ' + max_temperature[i] + '(High)' + '\n'

    full_script += '\n' + 'Data fetched on ' + create_date
    
    print('finish fetching data')
    return full_script

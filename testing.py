import requests

city = "Czech"

searchtext = "select item.condition from weather.forecast where woeid in (select woeid from geo.places(1) where text='" + city + "') and u='c'"

url = "https://query.yahooapis.com/v1/public/yql?q=" + searchtext + "&format=json"

data = requests.get(url).json()

print(data)

temp_info = data['query']['results']['channel']['item']['condition']['temp']
print("Temperature in " + i + " is" + " " + temp_info + "°C")

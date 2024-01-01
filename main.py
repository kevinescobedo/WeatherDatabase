import WeatherDatabase
import urllib.request
import secret
import json
import time
import random

def getCoordinates(city: str) -> tuple:
    """
    Gets the coordinates of a city
    """
    city = city.replace(" ", "+")

    link = f"https://api.openweathermap.org/geo/1.0/direct?q={city}&appid={secret.APIKEY}"
    request = urllib.request.Request(link, headers={"User-Agent": "Mozilla/5.0"})
    response = urllib.request.urlopen(request)
    data = json.loads(response.read())[0]
    return (data['lat'], data['lon'])

def getCurrentWeather(lat: float, lon: float) -> dict:
    """
    Uses the OpenWeather API to get the current weather
    """
    info = dict()

    link = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={secret.APIKEY}"
    request = urllib.request.Request(link, headers={"User-Agent": "Mozilla/5.0"})
    response = urllib.request.urlopen(request)
    data = json.loads(response.read())

    if data["cod"] == 200:
        info["city"] = data["name"]
        info["lat"] = data["coord"]["lat"]
        info["lon"] = data["coord"]["lon"]
        info["timestamp"] = data["dt"]
        info["description"] = data["weather"][0]["description"]
        info["temperature"] = data["main"]["temp"]
        info["feelsLike"] = data["main"]["feels_like"]
        info["pressure"] = data["main"]["pressure"]
        info["humidity"] = data["main"]["humidity"]
        info["windSpeed"] = data["wind"]["speed"]

    return info

if __name__ == "__main__":
    cities = ["Los Angeles", "New York"]
    weatherDatabase = WeatherDatabase.WeatherDatabase("weather.db")
    weatherDatabase.createCityTable()
    weatherDatabase.createWeatherTable()

    for city in cities:
        coordinates = weatherDatabase.getCityCoordinates(city)
        if coordinates is None:
            coordinates = getCoordinates(city)
            weatherDatabase.insertCity(city, coordinates[0], coordinates[1])

        weatherInfo = getCurrentWeather(coordinates[0], coordinates[1])
        weatherDatabase.insertWeatherData(weatherInfo)
        time.sleep(random.randint(10, 60))

    weatherDatabase.close()

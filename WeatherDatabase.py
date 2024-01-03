import sqlite3

class WeatherDatabase:
    def __init__(self, name: str):
        self.db = sqlite3.connect(name, detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES)
        self.cursor = self.db.cursor()

    def createCityTable(self) -> None:
        """
        Creates a city table with the following information:
            name TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL
        """
        command = "CREATE TABLE IF NOT EXISTS CITIES(NAME TEXT NOT NULL, LAT REAL NOT NULL, LON REAL NOT NULL, PRIMARY KEY(NAME, LAT, LON));"
        self.cursor.execute(command)
        self.flush()

    def insertCity(self, name: str, lat: float, lon: float) -> None:
        """
        Inserts a new city into the CITIES table
        """
        command = "INSERT INTO CITIES(NAME, LAT, LON) VALUES(?, ?, ?)"
        try:
            self.cursor.execute(command, (name, lat, lon))

        except sqlite3.IntegrityError:
            print(f"Cannot insert: {name}, {lat}, {lon}")

    def getCityCoordinates(self, name: str) -> tuple:
        """
        Runs a query to get the lat and lon of a city by name
        """
        command = "SELECT LAT, LON FROM CITIES WHERE NAME=?"
        self.cursor.execute(command, (name,))
        return self.cursor.fetchone()
    
    def createWeatherTable(self) -> None:
        """
        Creates a new WEATHER table with the following schema:
            TIMESTAMP: INT NOT NULL,
            CITY: TEXT NOT NULL,
            LAT: REAL NOT NULL,
            LON: REAL NOT NULL,
            DESCRIPTION: TEXT NOT NULL,
            TEMPERATURE: REAL NOT NULL,
            FEELSLIKE: REAL NOT NULL,
            PRESSURE: INT NOT NULL,
            HUMIDITY: INT NOT NULL,
            WINDSPEED: REAL NOT NULL,
            PRIMARY KEY(TIMESTAMP, CITY, LAT, LON)
        """
        command = "CREATE TABLE IF NOT EXISTS WEATHER(TIMESTAMP INT NOT NULL, CITY TEXT NOT NULL, LAT REAL NOT NULL, LON REAL NOT NULL, DESCRIPTION TEXT NOT NULL, TEMPERATURE REAL NOT NULL, FEELSLIKE REAL NOT NULL, PRESSURE INT NOT NULL, HUMIDITY INT NOT NULL, WINDSPEED REAL NOT NULL, PRIMARY KEY(TIMESTAMP, CITY, LAT, LON));"
        self.cursor.execute(command)
        self.flush()

    def insertWeatherData(self, weatherData: dict) -> None:
        """
        Inserts weather data into the WEATHER table
        """
        command = "INSERT INTO WEATHER(TIMESTAMP, CITY, LAT, LON, DESCRIPTION, TEMPERATURE, FEELSLIKE, PRESSURE, HUMIDITY, WINDSPEED) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"
        try:

            self.cursor.execute(command, (weatherData["timestamp"], weatherData["city"], weatherData["lat"], weatherData["lon"], weatherData["description"], weatherData["temperature"], weatherData["feelsLike"], weatherData["pressure"], weatherData["humidity"], weatherData["windSpeed"]))

        except sqlite3.IntegrityError:
            print(f'Cannot insert: {weatherData["timestamp"]}, {weatherData["city"]}, {weatherData["lat"]}, {weatherData["lon"]}, {weatherData["description"]}, {weatherData["temperature"]}, {weatherData["feelsLike"]}, {weatherData["pressure"]}, {weatherData["humidity"]}, {weatherData["windSpeed"]}')

    def getAverageTemperature(self, city: str, start: int|float, end: int|float) -> float:
        """
        Calculates the average temperature of a city between two timestamps
        """
        command = "SELECT AVG(TEMPERATURE) FROM WEATHER WHERE CITY=? AND TIMESTAMP >= ? AND TIMESTAMP <= ?"
        self.cursor.execute(command, (city, start, end))
        return self.cursor.fetchone()[0]

    def flush(self) -> None:
        """
        Flushes any outstanding database commits
        """
        self.db.commit()

    def close(self) -> None:
        """
        Closes the connection to the database
        """
        self.flush()
        self.db.close()

import argparse
from dateutil.parser import parse

from meta_weather_api import MetaWeatherApi

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to get the weather for") # string by default

    args = parser.parse_args()
    date = parse(args.date).date()

    meta_weather_api = MetaWeatherApi(weather_date=date)
    meta_weather_api.update_and_display_csv()

import requests
import configparser

config = configparser.ConfigParser()


url = "https://yfapi.net/v6/finance/quote"

querystring = {"symbols":"AAPL,BTC-USD,EURUSD=X"}

headers = {
    'x-api-key': config['YAHOO FINANCE']['KEY']
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
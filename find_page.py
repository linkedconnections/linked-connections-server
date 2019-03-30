#!/usr/bin/env python3
import requests
BASE_URI = "https://lc.dylanvanassche.be/sncb/connections?departureTime="

def find_page(uri):
    hydraNext = BASE_URI + "2019-03-15T14:00:00.000Z"
    while True:
        r = requests.get(hydraNext)
        data = r.json()
        print(data["@id"])
        for connection in data["@graph"]:
            if connection["@id"] == uri:
                print("Found connection")
                return connection, data["@id"]
        hydraNext = data["hydra:next"]


if __name__ == "__main__":
    connection, page = find_page("http://irail.be/connections/IC2336/20190316/8892205") 
    print(connection)
    print(page)

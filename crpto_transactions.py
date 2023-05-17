import requests
import csv
import time


url = "https://api.kucoin.com/api/v1/market/histories"
symbols = ["SUI-USDT"]

headers = {
    "KC-API-KEY": "API_KEY",
    "KC-API-SECRET": "API_SECRET"
}


batch_size = 100


data_dict = {symbol: [] for symbol in symbols}

while True:
    for symbol in symbols:
        params = {"symbol": symbol}

   
        response = requests.get(url, params=params, headers=headers)


        if response.status_code == 200:

            data = response.json()
            trades = data['data']


            for trade in trades:
                data_dict[symbol].append([trade['time'], trade['side'], trade['price'], trade['size']])

        else:
            print(f"Error {response.status_code}: {response.text}")


    time.sleep(1)

    for symbol, data_list in data_dict.items():
        if len(data_list) >= batch_size:

            filename = f"{symbol.lower()}_transactions.csv"
            with open(filename, 'a', newline='') as file:
                writer = csv.writer(file)


                if file.tell() == 0:
                    writer.writerow(['Time', 'Symbol', 'Side', 'Price', 'Size'])


                for row in data_list[:batch_size]:
                    writer.writerow(row)


                data_dict[symbol] = data_list[batch_size:]

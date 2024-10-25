import sys
import json
import time
import math
import random
import requests
import websocket
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def create_topic_if_not_exists(topic_name, bootstrap_servers):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {topic_name} created.")
    else:
        print(f"Topic {topic_name} already exists.")

class MockedFXProducer:
    def __init__(self):
        self.base_prices = {
            'EUR/USD': 1.0820,
            'USD/JPY': 151.50,
            'EUR/JPY': 164.00
        }
        self.counter = 0

    def get_current_prices(self):
        current_time = int(time.time() * 1000)
        self.counter += 1
        
        if self.counter % 10 == 0:  # Create obvious arbitrage opportunity
            # Create a clear arbitrage opportunity:
            # If EUR/USD = 1.0820 and USD/JPY = 151.50
            # Then EUR/JPY should be 1.0820 * 151.50 = 163.923
            # We'll set it higher to create obvious opportunity
            eur_usd = self.base_prices['EUR/USD']
            usd_jpy = self.base_prices['USD/JPY']
            theoretical_eur_jpy = eur_usd * usd_jpy
            eur_jpy = theoretical_eur_jpy * 1.01  # 1% profit opportunity
            
            prices = [
                {'symbol': 'EUR/USD', 'price': eur_usd, 'timestamp': current_time},
                {'symbol': 'USD/JPY', 'price': usd_jpy, 'timestamp': current_time},
                {'symbol': 'EUR/JPY', 'price': eur_jpy, 'timestamp': current_time}
            ]
        else:
            # Normal prices without arbitrage
            eur_usd = self.base_prices['EUR/USD']
            usd_jpy = self.base_prices['USD/JPY']
            theoretical_eur_jpy = eur_usd * usd_jpy
            
            prices = [
                {'symbol': 'EUR/USD', 'price': eur_usd * (1 + random.uniform(-0.0001, 0.0001)), 'timestamp': current_time},
                {'symbol': 'USD/JPY', 'price': usd_jpy * (1 + random.uniform(-0.0001, 0.0001)), 'timestamp': current_time},
                {'symbol': 'EUR/JPY', 'price': theoretical_eur_jpy * (1 + random.uniform(-0.0001, 0.0001)), 'timestamp': current_time}
            ]
        
        return prices

def on_message(ws, message, producer, topic_name, profile):
    raw_data = json.loads(message)
    if raw_data['type'] == 'trade':
        for trade in raw_data['data']:
            symbol = trade['s'].replace('OANDA:', '').replace('_', '/')  # Replace underscore with slash
            standardized_data = {
                'type': 'forex',
                'source': profile,
                'data': {
                    'symbol': symbol,
                    'price': trade['p'],
                    'timestamp': trade['t']
                }
            }
            producer.send(topic_name, value=standardized_data)
            producer.flush()
            print(f"Sent data to {topic_name}: {standardized_data}")

def on_open(ws, tickers):
    print(f"Subscribing to tickers: {tickers}")
    for ticker in tickers:
        ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))

def fetch_forex_data(api_type, url, params, profile):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        
        if api_type == "alphavantage":
            if "Realtime Currency Exchange Rate" in data:
                exchange_rate = data["Realtime Currency Exchange Rate"]
                return {
                    'type': 'forex',
                    'source': profile,
                    'data': {
                        'symbol': f"{params['from_currency']}/{params['to_currency']}",
                        'price': float(exchange_rate["5. Exchange Rate"]),
                        'timestamp': int(time.time() * 1000)
                    }
                }
    return None

def run_websocket_producer(config, profile_config, producer, profile):
    topic_name = profile_config['topic']
    api_key = profile_config['api_key']
    ws_url = profile_config['ws_url'].replace("{API_KEY}", api_key)
    tickers = profile_config['tickers']

    ws = websocket.WebSocketApp(
        ws_url,
        on_message=lambda ws, message: on_message(ws, message, producer, topic_name, profile),
        on_open=lambda ws: on_open(ws, tickers)
    )

    print(f"Connecting to {profile_config['api_type']} WebSocket at {ws_url}")
    ws.run_forever()

def run_polling_producer(config, profile_config, producer, profile):
    topic_name = profile_config['topic']
    api_key = profile_config['api_key']
    api_type = profile_config['api_type']
    base_url = profile_config['base_url']
    pairs = profile_config['tickers']
    polling_interval = profile_config.get('polling_interval', 60)

    print(f"Starting {api_type} polling for forex rates")
    
    while True:
        for pair in pairs:
            if api_type == "alphavantage":
                from_currency, to_currency = pair.split('/')
                params = {
                    "function": "CURRENCY_EXCHANGE_RATE",
                    "from_currency": from_currency,
                    "to_currency": to_currency,
                    "apikey": api_key
                }
            
            data = fetch_forex_data(api_type, base_url, params, profile)
            
            if data:
                producer.send(topic_name, value=data)
                producer.flush()
                print(f"Sent data to {topic_name}: {data}")
        
        time.sleep(polling_interval)

def run_mocked_producer(config, profile_config, producer, profile):
    topic_name = profile_config['topic']
    mocked_fx = MockedFXProducer()
    
    print(f"Starting {profile} mocked FX data generation")
    print(f"Arbitrage opportunities will be generated every 10 seconds with 1% profit")
    
    while True:
        prices = mocked_fx.get_current_prices()
        
        for price_data in prices:
            data = {
                'type': 'forex',
                'source': profile,
                'data': price_data
            }
            producer.send(topic_name, value=data)
            producer.flush()
            print(f"Sent data to {topic_name}: {data}")
        
        time.sleep(1)  # Generate data every second

def run_producer(config, profile):
    profile_config = config['fx_profiles'][profile]
    topic_name = profile_config['topic']
    bootstrap_servers = config['bootstrap_servers']

    create_topic_if_not_exists(topic_name, bootstrap_servers)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if profile_config['api_type'] == 'mockedfx':
        run_mocked_producer(config, profile_config, producer, profile)
    elif 'ws_url' in profile_config:
        run_websocket_producer(config, profile_config, producer, profile)
    else:
        run_polling_producer(config, profile_config, producer, profile)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fx_producer.py <config.json> <profile>")
        sys.exit(1)

    config_path = sys.argv[1]
    profile = sys.argv[2]

    with open(config_path, 'r') as file:
        config = json.load(file)

    if profile not in config['fx_profiles']:
        print(f"Profile '{profile}' not found in config.json.")
        sys.exit(1)

    run_producer(config, profile)

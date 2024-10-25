# Foreign Exchange (Fx) Triangular Arbitrage
This project shows how to use kafka to find out historical and realtime opportunities for the simplest case of FX arbitrage, the triangular arbitrage.
![Triangular Arbitrage Visualization](https://raw.githubusercontent.com/nestoru/kafka-fx-triangular-arbitrage/main/triangular_arbitrage_opportunities.gif)

```
├── README.md
├── consumer.py
├── docker-compose.yml
├── fx_producer.py
├── producer.py
├── triangular_arbitrage_opportunities.gif
├── triangular_arbitrage_tracker.py
└── triangular_arbitrage_visualizer.py
```

The triangular_arbitrage_visualizer.py script allows to visualize the FX data stored in sqlite or consumed realtime from an arbitrage opportunity datafeed published in a kafka topic. This topic is published by triangular_arbitrage_tracker.py which consumes data from a normalized FX datafeed topic. The FX datafeed topic data can be published from a number of external datafeeds which is the job of the fx_producer.py, like for example finnhub and alphavantage.
 
While finnhub and alphavantage free development APIs will allow to play a bit with real services the chances to seeing an opportunity for arbitrage with their free version is not high. This is why I include a mocked FX datafeed produces which sends messages to the kafka topic.

Triangular arbitrage involves trading three different currency pairs in a closed loop to exploit any discrepancies in the exchange rates that can yield a profit.

First step is to identify three exchange rate definitions:

EUR/USD: This tells you how many USD you can buy for 1 EUR.
USD/JPY: This tells you how many JPY you can buy for 1 USD.
EUR/JPY: This tells you how many JPY you can buy for 1 EUR.

The implied rate for EUR/JPY can be calculated by multiplying EUR/USD and USD/JPY:
```
Implied EUR/JPY = USD/JPY x EUR/USD
``` 
If the actual EUR/JPY rate differs from this implied cross-rate, an arbitrage opportunity may exist.

For example, start with 1 EUR, convert 1 EUR to 1.11 USD using the EUR/USD rate, convert 1.11 USD to 168.29 JPY using the USD/JPY rate, convert 168.29 JPY to 1.03 EUR using the EUR/JPY rate. You’ve ended up with 1.03 EUR from an initial 1 EUR investment, yielding a 3-cent profit, which represents the arbitrage opportunity.

## Preconditions
Sample .config.json
```
{
  "fx_profiles": {
    "finnhub": {
      "api_type": "finnhub",
      "api_key": "your_finnhub_api_key",
      "ws_url": "wss://ws.finnhub.io?token={API_KEY}",
      "topic": "finnhub_topic",
      "tickers": ["OANDA:EUR_USD", "OANDA:USD_JPY", "OANDA:EUR_JPY"]  # Correct tickers for triangular arbitrage
    },
    "alphavantage": {
      "api_type": "alphavantage",
      "api_key": "YOUR_ALPHA_VANTAGE_KEY",
      "base_url": "https://www.alphavantage.co/query",
      "topic": "alphavantage_topic",
      "tickers": ["EUR/USD", "USD/JPY", "EUR/JPY"],
      "polling_interval": 60
    },
    "mockedfx": {
      "api_type": "mockedfx",
      "topic": "mockedfx_topic",
      "tickers": ["EUR/USD", "USD/JPY", "EUR/JPY"]
    }
  },
  "bootstrap_servers": "localhost:9092"
}
```

## Installing docker in OSX
```
brew install --cask docker
```

## Running docker in OSX
```
open -a Docker # complete any missing steps from the GUI before you are able to use docker-compose
```

## Running kafka and zookeeper from docker in OSX
For Linux you will need to change the platform in docker-compose.yml
```
docker-compose down
docker-compose up -d
```

## Producers
* A sample producer.py allows you to test your kafka broker,
```
python producer.py
```
* The fx_producer.py parses a .config.json to receive FX information from data feeds based on a profile name. It then send messages to a kafka topic. If the topic does not exist then it creates it.
```
python fx_producer.py .config.json finnhub
python fx_producer.py .config.json alphavantage
python fx_producer.py .config.json mockedfx
```
* The triangular_arbitrage_tracker.py is both a producer and a consumer. It produces the arbitrage opportunities as it consumes the FX rates publsihed by the FX producer. It receives a topic and a sqlite path:
```
python triangular_arbitrage_tracker.py mockedfx_topic ~/Downloads/mockedfx_arbitrage_opportunities.sqlite
```

### FX datafeeds
* finhub: A free account allows you to use websockets to receive fx rates.
* alphavantage: A free account allows you to use polling to receive fx rates.
* mockedfx: A datafeed implementation that generates random prices for the selected tickers.
* code should be extended to manage other datafeeds as needed.

## Consumers
* To consume from a topic for testing purposes from inside the kafka container use a command like the below:
```
kafka-console-consumer --bootstrap-server localhost:9092 --topic finnhub_topic --from-beginning
```
* The sample consumer.py can subscribe to any topic, for example:
```
python consumer.py 'finnhub_topic'
```
* To visualize arbitrage opportunities the triangular_arbitrage_visualizer.py consumer subscribes to a topic (where the triangular_arbitrage_tracker.py published the detected good investments). It can be run to analyze historical opportunities or to look realtime at current arbitrage opportunities:
```
# Historical data only
python triangular_arbitrage_visualizer.py triangular_arbitrage ~/Downloads/mockedfx_arbitrage_opportunities.sqlite 0.1 1000 "2024-02-25-00:00:00" "2024-02-25-23:59:59"

# Real-time updating
python triangular_arbitrage_visualizer.py triangular_arbitrage ~/Downloads/mockedfx_arbitrage_opportunities.sqlite 0.1 1000 "2024-02-25-00:00:00"
```


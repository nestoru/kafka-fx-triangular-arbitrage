# triangular_arbitrage_tracker.py
import sys
import json
import sqlite3
from datetime import datetime
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

class ArbitrageTracker:
    def __init__(self, input_topic, sqlite_path, bootstrap_servers='localhost:9092'):
        self.input_topic = input_topic
        self.sqlite_path = sqlite_path
        self.bootstrap_servers = bootstrap_servers
        self.output_topic = 'triangular_arbitrage'
        self.price_cache = defaultdict(dict)  # {source: {symbol: {'price': price, 'timestamp': ts}}}
        self.active_opportunities = {}  # {opportunity_id: opportunity_data}
        self.setup_kafka()
        self.setup_database()

    def setup_kafka(self):
        # Create output topic if it doesn't exist
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        existing_topics = admin_client.list_topics()
        
        if self.output_topic not in existing_topics:
            topic_list = [NewTopic(name=self.output_topic, num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic {self.output_topic} created.")

        # Setup consumer and producer
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def setup_database(self):
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                    id TEXT PRIMARY KEY,
                    source TEXT,
                    trio TEXT,
                    profit REAL,
                    start_timestamp INTEGER,
                    end_timestamp INTEGER
                )
            ''')

    def find_triangular_opportunities(self, source):
        opportunities = []
        prices = self.price_cache[source]
        
        # Get all currency pairs for this source
        pairs = list(prices.keys())
        for i in range(len(pairs)):
            for j in range(len(pairs)):
                for k in range(len(pairs)):
                    if i != j and j != k and i != k:
                        opp = self.check_arbitrage(pairs[i], pairs[j], pairs[k], prices)
                        if opp:
                            opportunities.append(opp)
        return opportunities

    def check_arbitrage(self, pair1, pair2, pair3, prices):
        # Get prices and timestamps
        p1_data = prices.get(pair1)
        p2_data = prices.get(pair2)
        p3_data = prices.get(pair3)

        if not all([p1_data, p2_data, p3_data]):
            return None

        p1_base, p1_quote = pair1.split('/')
        p2_base, p2_quote = pair2.split('/')
        p3_base, p3_quote = pair3.split('/')

        # Check valid triangle (e.g., EUR/USD, USD/JPY, EUR/JPY)
        if not ((p1_base == p3_base and p1_quote == p2_base and p2_quote == p3_quote) or
                (p1_quote == p2_base and p2_quote == p3_base and p3_quote == p1_base)):
            return None

        # Calculate cross-rate and compare
        rate1 = p1_data['price']
        rate2 = p2_data['price']
        rate3 = p3_data['price']

        # Try both directions
        # Direction 1: e.g., buy EUR with USD, buy JPY with USD, sell JPY for EUR
        profit1 = (1 / rate1) * (1 / rate2) * rate3 - 1

        # Direction 2: e.g., buy USD with EUR, buy JPY with USD, sell JPY for EUR
        profit2 = rate1 * rate2 * (1 / rate3) - 1

        profit_percentage = max(profit1, profit2) * 100

        if profit_percentage > 0.1:  # Only report opportunities with >0.1% profit
            timestamp = max(p1_data['timestamp'], p2_data['timestamp'], p3_data['timestamp'])
            return {
                'trio': f"{pair1}_{pair2}_{pair3}",
                'profit': profit_percentage,
                'timestamp': timestamp
            }
        return None

    def update_opportunities(self, source, new_opportunities):
        current_time = int(datetime.now().timestamp() * 1000)
        
        # Update or close existing opportunities
        for opp_id in list(self.active_opportunities.keys()):
            opp = self.active_opportunities[opp_id]
            if opp['source'] != source:
                continue
                
            trio = opp['trio']
            still_active = False
            
            for new_opp in new_opportunities:
                if new_opp['trio'] == trio:
                    # Update the opportunity
                    opp['profit'] = new_opp['profit']
                    still_active = True
                    break
            
            if not still_active:
                # Close the opportunity
                opp['end_timestamp'] = current_time
                self.save_opportunity(opp)
                self.producer.send(self.output_topic, value=opp)
                print(f"Closed arbitrage opportunity: {opp}")
                del self.active_opportunities[opp_id]

        # Start new opportunities
        for new_opp in new_opportunities:
            trio = new_opp['trio']
            if not any(opp['trio'] == trio and opp['source'] == source 
                      for opp in self.active_opportunities.values()):
                opp_id = f"{source}_{trio}_{current_time}"
                opportunity = {
                    'id': opp_id,
                    'source': source,
                    'trio': trio,
                    'profit': new_opp['profit'],
                    'start_timestamp': current_time,
                    'end_timestamp': None
                }
                self.active_opportunities[opp_id] = opportunity
                print(f"Found new arbitrage opportunity: {opportunity}")

    def save_opportunity(self, opportunity):
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute('''
                INSERT INTO arbitrage_opportunities 
                (id, source, trio, profit, start_timestamp, end_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                opportunity['id'],
                opportunity['source'],
                opportunity['trio'],
                opportunity['profit'],
                opportunity['start_timestamp'],
                opportunity['end_timestamp']
            ))

    def run(self):
        print(f"Starting arbitrage tracker... Listening to topic: {self.input_topic}")
        for message in self.consumer:
            data = message.value
            if data['type'] == 'forex':
                source = data['source']
                symbol = data['data']['symbol']
                price = data['data']['price']
                timestamp = data['data']['timestamp']

                # Update price cache
                self.price_cache[source][symbol] = {
                    'price': price,
                    'timestamp': timestamp
                }

                # Find new opportunities
                new_opportunities = self.find_triangular_opportunities(source)
                
                # Update active opportunities
                self.update_opportunities(source, new_opportunities)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python triangular_arbitrage_tracker.py <topic> <sqlite_path>")
        sys.exit(1)

    topic = sys.argv[1]
    sqlite_path = sys.argv[2]

    tracker = ArbitrageTracker(topic, sqlite_path)
    tracker.run()

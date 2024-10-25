# triangular_arbitrage_visualizer.py
import sys
import json
import sqlite3
from datetime import datetime, timezone, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
import numpy as np
from matplotlib.dates import DateFormatter
from matplotlib.dates import AutoDateLocator

class ArbitrageVisualizer:
    def __init__(self, topic, sqlite_path, min_profit, min_duration, start_time, end_time=None, bootstrap_servers='localhost:9092'):
        self.topic = topic
        self.sqlite_path = sqlite_path
        self.min_profit = float(min_profit)
        self.min_duration = int(min_duration)
        self.start_time = int(start_time)
        self.end_time = int(end_time) if end_time else None
        self.bootstrap_servers = bootstrap_servers
        
        # For plotting
        plt.style.use('dark_background')
        self.fig, self.ax = plt.subplots(figsize=(15, 8))
        self.color_map = {}
        self.next_color = 0
        self.colors = [
            '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#bcbd22', '#17becf'
        ]
        
        # Set window size (in minutes) for real-time view
        self.window_minutes = 5
        
        # Initialize empty DataFrame
        self.df = pd.DataFrame(columns=['timestamp', 'profit', 'trio'])
        
        # Initialize consumer if needed
        if not end_time or int(end_time) > int(datetime.now(timezone.utc).timestamp() * 1000):
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                group_id=None,
                auto_offset_reset='latest'
            )
            print(f"Consumer initialized for topic: {self.topic}")
        else:
            self.consumer = None

    def get_color(self, trio):
        if trio not in self.color_map:
            self.color_map[trio] = self.colors[self.next_color % len(self.colors)]
            self.next_color += 1
        return self.color_map[trio]

    def load_historical_data(self):
        query = '''
        SELECT id, source, trio, profit, start_timestamp
        FROM arbitrage_opportunities
        WHERE start_timestamp >= ?
        AND profit >= ?
        '''
        params = [self.start_time, self.min_profit]
        
        if self.end_time:
            query += ' AND start_timestamp <= ?'
            params.append(self.end_time)
            
        with sqlite3.connect(self.sqlite_path) as conn:
            df = pd.read_sql_query(query, conn, params=params)
        
        if not df.empty:
            # Convert to timezone-aware timestamps
            df['timestamp'] = pd.to_datetime(df['start_timestamp'], unit='ms', utc=True)
        
        return df

    def animate(self, frame):
        try:
            # Ensure current_time is timezone-aware
            current_time = pd.Timestamp.now(tz='UTC')
            
            # Process new messages if consumer exists
            if self.consumer:
                messages = self.consumer.poll(timeout_ms=100)
                for topic_partition, msg_list in messages.items():
                    for message in msg_list:
                        opportunity = message.value
                        print(f"Received message: {opportunity}")
                        
                        if opportunity['profit'] >= self.min_profit:
                            # Create timezone-aware timestamp
                            timestamp = pd.to_datetime(opportunity['start_timestamp'], unit='ms', utc=True)
                            new_data = {
                                'timestamp': timestamp,
                                'profit': opportunity['profit'],
                                'trio': opportunity['trio']
                            }
                            self.df = pd.concat([self.df, pd.DataFrame([new_data])], ignore_index=True)
                            print(f"Added new opportunity: {new_data}")

            # Clear previous plot
            self.ax.clear()
            
            # Calculate window
            window_start = current_time - pd.Timedelta(minutes=self.window_minutes)
            
            # Filter visible data
            visible_data = self.df[
                (self.df['timestamp'] >= window_start) & 
                (self.df['timestamp'] <= current_time)
            ]
            
            # Debug print
            print(f"Window: {window_start} to {current_time}")
            print(f"Total points: {len(self.df)}, Visible points: {len(visible_data)}")
            
            # Set fixed window
            self.ax.set_xlim(window_start, current_time)
            
            # Plot data if exists
            if not visible_data.empty:
                for trio in visible_data['trio'].unique():
                    trio_data = visible_data[visible_data['trio'] == trio]
                    self.ax.scatter(trio_data['timestamp'],
                                  trio_data['profit'],
                                  color=self.get_color(trio),
                                  label=trio,
                                  s=50,
                                  alpha=0.8)
                
                # Update y-axis with padding
                y_min = visible_data['profit'].min()
                y_max = visible_data['profit'].max()
                padding = max((y_max - y_min) * 0.1, 0.1)
                self.ax.set_ylim(max(0, y_min - padding), y_max + padding)
            else:
                # Default y-axis if no data
                self.ax.set_ylim(0, 2)
            
            # Grid and formatting
            self.ax.grid(True, alpha=0.3)
            self.ax.set_title(f'Triangular Arbitrage Opportunities\nMin Profit: {self.min_profit}%')
            self.ax.set_xlabel('Time')
            self.ax.set_ylabel('Profit (%)')
            
            # Time formatting
            self.ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
            plt.xticks(rotation=45)
            
            # Add legend if we have data
            if not visible_data.empty:
                self.ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title='Arbitrage Trios')
            
            # Adjust layout
            plt.tight_layout()
            
        except Exception as e:
            print(f"Error in animation: {e}")

    def run(self):
        print(f"Loading historical data from {self.sqlite_path}...")
        historical_data = self.load_historical_data()
        if not historical_data.empty:
            self.df = pd.concat([self.df, historical_data], ignore_index=True)
        print(f"Found {len(self.df)} historical opportunities meeting criteria")
        
        if self.consumer:
            print(f"Starting real-time monitoring of topic: {self.topic}")
            ani = animation.FuncAnimation(
                self.fig, 
                self.animate,
                interval=500,
                cache_frame_data=False,
                blit=False
            )
        else:
            print("Showing historical data only...")
            self.animate(None)
        
        plt.show()

def parse_timestamp(timestamp_str):
    try:
        dt = datetime.strptime(timestamp_str, '%Y-%m-%d-%H:%M:%S')
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        print("Error: Timestamp should be in format YYYY-MM-DD-HH:mm:ss")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) not in [6, 7]:
        print("Usage: python triangular_arbitrage_visualizer.py <topic> <sqlite_path> <minimum_profit> <minimum_duration_ms> <start_time> [<end_time>]")
        print("Time format: YYYY-MM-DD-HH:mm:ss")
        sys.exit(1)

    topic = sys.argv[1]
    sqlite_path = sys.argv[2]
    min_profit = float(sys.argv[3])
    min_duration = int(sys.argv[4])
    start_time = parse_timestamp(sys.argv[5])
    end_time = parse_timestamp(sys.argv[6]) if len(sys.argv) == 7 else None

    visualizer = ArbitrageVisualizer(
        topic=topic,
        sqlite_path=sqlite_path,
        min_profit=min_profit,
        min_duration=min_duration,
        start_time=start_time,
        end_time=end_time
    )
    visualizer.run()

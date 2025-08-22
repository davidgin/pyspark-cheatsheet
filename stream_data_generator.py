import time
import random
import json
import csv
import os
from datetime import datetime, timedelta
import threading

class StreamDataGenerator:
    """Generates streaming data for PySpark Structured Streaming practice"""
    
    def __init__(self, output_dir="streaming_data"):
        self.output_dir = output_dir
        self.running = False
        os.makedirs(output_dir, exist_ok=True)
        
        # Sample data for generation
        self.users = list(range(1, 51))  # 50 users
        self.products = [
            "laptop", "mouse", "keyboard", "monitor", "headphones",
            "tablet", "phone", "charger", "speaker", "webcam"
        ]
        self.actions = ["view", "click", "add_to_cart", "purchase", "remove_from_cart"]
        self.cities = [
            "New York", "San Francisco", "Chicago", "Boston", "Seattle",
            "Austin", "Denver", "Miami", "Portland", "Atlanta"
        ]

    def generate_user_activity_csv(self, file_count=5, records_per_file=10):
        """Generate CSV files with user activity data"""
        print(f"Generating {file_count} CSV files with user activity...")
        
        for i in range(file_count):
            filename = f"{self.output_dir}/user_activity_{i+1:03d}.csv"
            
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["timestamp", "user_id", "action", "product", "value"])
                
                base_time = datetime.now() + timedelta(minutes=i*2)
                
                for j in range(records_per_file):
                    timestamp = base_time + timedelta(seconds=j*10)
                    user_id = random.choice(self.users)
                    action = random.choice(self.actions)
                    product = random.choice(self.products)
                    value = round(random.uniform(1.0, 999.99), 2) if action == "purchase" else round(random.uniform(0.1, 10.0), 2)
                    
                    writer.writerow([
                        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        user_id,
                        action,
                        product,
                        value
                    ])
            
            print(f"Created {filename}")
            time.sleep(1)  # Small delay between files

    def generate_transaction_stream_csv(self, file_count=3, records_per_file=15):
        """Generate CSV files with transaction data for windowed aggregation"""
        print(f"Generating {file_count} CSV files with transaction data...")
        
        for i in range(file_count):
            filename = f"{self.output_dir}/transactions_{i+1:03d}.csv"
            
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["event_time", "user_id", "product_id", "amount", "city"])
                
                base_time = datetime.now() + timedelta(minutes=i*5)
                
                for j in range(records_per_file):
                    event_time = base_time + timedelta(seconds=j*20)
                    user_id = random.choice(self.users)
                    product_id = f"PROD_{random.randint(1, 20):03d}"
                    amount = round(random.uniform(10.0, 500.0), 2)
                    city = random.choice(self.cities)
                    
                    writer.writerow([
                        event_time.strftime("%Y-%m-%d %H:%M:%S"),
                        user_id,
                        product_id,
                        amount,
                        city
                    ])
            
            print(f"Created {filename}")
            time.sleep(2)  # Delay between files

    def generate_json_stream(self, duration_minutes=5, interval_seconds=3):
        """Generate JSON files continuously for specified duration"""
        print(f"Generating JSON stream for {duration_minutes} minutes...")
        
        start_time = time.time()
        file_counter = 1
        
        while time.time() - start_time < duration_minutes * 60:
            filename = f"{self.output_dir}/events_{file_counter:03d}.json"
            
            events = []
            for _ in range(random.randint(3, 8)):
                event = {
                    "timestamp": datetime.now().isoformat(),
                    "user_id": random.choice(self.users),
                    "session_id": f"session_{random.randint(1000, 9999)}",
                    "event_type": random.choice(["page_view", "button_click", "form_submit", "logout"]),
                    "page": random.choice(["/home", "/products", "/cart", "/checkout", "/profile"]),
                    "duration_ms": random.randint(100, 5000)
                }
                events.append(event)
            
            with open(filename, 'w') as jsonfile:
                for event in events:
                    jsonfile.write(json.dumps(event) + "\n")
            
            print(f"Created {filename} with {len(events)} events")
            file_counter += 1
            time.sleep(interval_seconds)

    def start_continuous_generation(self, interval_seconds=10):
        """Start continuous data generation in background thread"""
        self.running = True
        
        def generate_data():
            file_counter = 1
            while self.running:
                filename = f"{self.output_dir}/live_data_{file_counter:03d}.csv"
                
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(["timestamp", "user_id", "action", "value", "session_id"])
                    
                    # Generate random number of records
                    num_records = random.randint(5, 15)
                    
                    for _ in range(num_records):
                        timestamp = datetime.now()
                        user_id = random.choice(self.users)
                        action = random.choice(self.actions)
                        value = round(random.uniform(1.0, 100.0), 2)
                        session_id = f"session_{random.randint(1000, 9999)}"
                        
                        writer.writerow([
                            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                            user_id,
                            action,
                            value,
                            session_id
                        ])
                
                print(f"Generated continuous data file: {filename}")
                file_counter += 1
                time.sleep(interval_seconds)
        
        thread = threading.Thread(target=generate_data)
        thread.daemon = True
        thread.start()
        print("Started continuous data generation in background...")
        return thread

    def stop_continuous_generation(self):
        """Stop continuous data generation"""
        self.running = False
        print("Stopped continuous data generation.")

def main():
    print("PySpark Streaming Data Generator")
    print("=" * 40)
    
    generator = StreamDataGenerator()
    
    print("\nChoose data generation option:")
    print("1. Generate batch CSV files (for file stream practice)")
    print("2. Generate transaction data (for windowed aggregation)")
    print("3. Generate JSON stream (continuous for 5 minutes)")
    print("4. Start continuous CSV generation (runs in background)")
    print("5. Generate all types")
    
    try:
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            generator.generate_user_activity_csv()
        elif choice == "2":
            generator.generate_transaction_stream_csv()
        elif choice == "3":
            generator.generate_json_stream()
        elif choice == "4":
            thread = generator.start_continuous_generation()
            print("Press Enter to stop continuous generation...")
            input()
            generator.stop_continuous_generation()
        elif choice == "5":
            generator.generate_user_activity_csv(file_count=3)
            generator.generate_transaction_stream_csv(file_count=2)
            print("All data types generated!")
        else:
            print("Invalid choice")
            
    except KeyboardInterrupt:
        generator.stop_continuous_generation()
        print("\nData generation stopped.")

if __name__ == "__main__":
    main()
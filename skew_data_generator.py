import random
import csv
import os
from datetime import datetime, timedelta

class SkewedDataGenerator:
    """Generates datasets with various types of data skew for testing optimization techniques"""
    
    def __init__(self, output_dir: str = "skewed_data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Common data for generation
        self.cities = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
            "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
            "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte"
        ]
        
        self.products = [
            "iPhone", "Samsung Galaxy", "iPad", "MacBook", "Dell Laptop",
            "HP Printer", "Canon Camera", "Sony Headphones", "Nike Shoes",
            "Adidas Sneakers", "Levi Jeans", "Coffee Maker", "Microwave",
            "Refrigerator", "TV", "Gaming Console", "Tablet", "Smartwatch"
        ]
        
        self.departments = [
            "Electronics", "Clothing", "Home", "Sports", "Books",
            "Beauty", "Automotive", "Garden", "Health", "Toys"
        ]

    def generate_zipfian_skew_data(self, total_records: int = 100000, 
                                  skew_factor: float = 1.5) -> None:
        """Generate data with Zipfian distribution (realistic skew pattern)"""
        print(f"Generating Zipfian skewed data with {total_records} records...")
        
        # Generate Zipfian distribution for product popularity
        num_products = len(self.products)
        weights = [1.0 / (i ** skew_factor) for i in range(1, num_products + 1)]
        total_weight = sum(weights)
        probabilities = [w / total_weight for w in weights]
        
        filename = f"{self.output_dir}/zipfian_skewed_sales.csv"
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["order_id", "product_name", "customer_id", "city", 
                           "quantity", "price", "order_date", "category"])
            
            for i in range(total_records):
                # Select product based on Zipfian distribution
                product_idx = random.choices(range(num_products), weights=probabilities)[0]
                product = self.products[product_idx]
                
                order_id = f"ORD_{i+1:06d}"
                customer_id = random.randint(1, 50000)
                city = random.choice(self.cities)
                quantity = random.randint(1, 5)
                price = round(random.uniform(10, 2000), 2)
                
                # Generate dates over the last year
                base_date = datetime.now() - timedelta(days=365)
                random_days = random.randint(0, 365)
                order_date = (base_date + timedelta(days=random_days)).strftime("%Y-%m-%d")
                
                category = random.choice(self.departments)
                
                writer.writerow([order_id, product, customer_id, city, 
                               quantity, price, order_date, category])
        
        print(f"[SUCCESS] Created {filename}")
        self._print_distribution_stats(filename, "product_name")

    def generate_temporal_skew_data(self, total_records: int = 50000) -> None:
        """Generate data with temporal skew (Black Friday, holiday effects)"""
        print(f"Generating temporal skewed data with {total_records} records...")
        
        filename = f"{self.output_dir}/temporal_skewed_events.csv"
        
        # Define high-traffic periods
        black_friday = datetime(2024, 11, 29)
        cyber_monday = datetime(2024, 12, 2)
        christmas = datetime(2024, 12, 25)
        new_year = datetime(2024, 12, 31)
        
        high_traffic_dates = [black_friday, cyber_monday, christmas, new_year]
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["event_id", "user_id", "event_type", "timestamp", 
                           "page_views", "session_duration", "purchase_amount"])
            
            for i in range(total_records):
                # 60% of events on high-traffic days (temporal skew)
                if random.random() < 0.6:
                    base_date = random.choice(high_traffic_dates)
                    # Add some randomness within the day
                    timestamp = base_date + timedelta(
                        hours=random.randint(0, 23),
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59)
                    )
                else:
                    # Distribute remaining 40% across other days
                    start_date = datetime(2024, 1, 1)
                    end_date = datetime(2024, 12, 31)
                    time_between = end_date - start_date
                    days_between = time_between.days
                    random_days = random.randrange(days_between)
                    timestamp = start_date + timedelta(days=random_days)
                
                event_id = f"EVT_{i+1:06d}"
                user_id = random.randint(1, 10000)
                event_type = random.choice(["view", "click", "purchase", "add_to_cart"])
                page_views = random.randint(1, 20)
                session_duration = random.randint(30, 3600)  # seconds
                purchase_amount = round(random.uniform(0, 500), 2) if event_type == "purchase" else 0
                
                writer.writerow([event_id, user_id, event_type, 
                               timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                               page_views, session_duration, purchase_amount])
        
        print(f"[SUCCESS] Created {filename}")

    def generate_geographic_skew_data(self, total_records: int = 75000) -> None:
        """Generate data with geographic skew (80/20 rule by location)"""
        print(f"Generating geographic skewed data with {total_records} records...")
        
        filename = f"{self.output_dir}/geographic_skewed_customers.csv"
        
        # Create geographic skew: 80% from top 3 cities
        major_cities = ["New York", "Los Angeles", "Chicago"]
        other_cities = [city for city in self.cities if city not in major_cities]
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["customer_id", "name", "city", "state", "zip_code", 
                           "signup_date", "total_orders", "lifetime_value"])
            
            for i in range(total_records):
                customer_id = f"CUST_{i+1:06d}"
                name = f"Customer_{i+1}"
                
                # 80% from major cities, 20% from others
                if random.random() < 0.8:
                    city = random.choice(major_cities)
                else:
                    city = random.choice(other_cities)
                
                # Map cities to states (simplified)
                city_state_map = {
                    "New York": "NY", "Los Angeles": "CA", "Chicago": "IL",
                    "Houston": "TX", "Phoenix": "AZ", "Philadelphia": "PA"
                }
                state = city_state_map.get(city, "CA")
                
                zip_code = f"{random.randint(10000, 99999)}"
                
                # Random signup date in last 2 years
                signup_date = (datetime.now() - timedelta(days=random.randint(1, 730))).strftime("%Y-%m-%d")
                
                total_orders = random.randint(1, 50)
                lifetime_value = round(random.uniform(50, 5000), 2)
                
                writer.writerow([customer_id, name, city, state, zip_code,
                               signup_date, total_orders, lifetime_value])
        
        print(f"[SUCCESS] Created {filename}")
        self._print_distribution_stats(filename, "city")

    def generate_user_behavior_skew_data(self, total_records: int = 200000) -> None:
        """Generate data with user behavior skew (power users vs casual users)"""
        print(f"Generating user behavior skewed data with {total_records} records...")
        
        filename = f"{self.output_dir}/user_behavior_skewed.csv"
        
        # Create user segments
        num_users = 10000
        power_users = list(range(1, 101))  # Top 1% are power users
        active_users = list(range(101, 1001))  # Next 9% are active users
        casual_users = list(range(1001, num_users + 1))  # Remaining 90% are casual
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["session_id", "user_id", "user_type", "page_views", 
                           "clicks", "time_spent", "actions", "date"])
            
            for i in range(total_records):
                session_id = f"SESS_{i+1:08d}"
                
                # Distribute sessions based on user behavior patterns
                rand = random.random()
                if rand < 0.5:  # 50% of sessions from power users (1% of users)
                    user_id = random.choice(power_users)
                    user_type = "power"
                    page_views = random.randint(20, 100)
                    clicks = random.randint(10, 50)
                    time_spent = random.randint(1800, 7200)  # 30min to 2hr
                    actions = random.randint(15, 75)
                elif rand < 0.8:  # 30% from active users (9% of users)
                    user_id = random.choice(active_users)
                    user_type = "active"
                    page_views = random.randint(5, 25)
                    clicks = random.randint(3, 15)
                    time_spent = random.randint(300, 1800)  # 5min to 30min
                    actions = random.randint(3, 20)
                else:  # 20% from casual users (90% of users)
                    user_id = random.choice(casual_users)
                    user_type = "casual"
                    page_views = random.randint(1, 8)
                    clicks = random.randint(0, 5)
                    time_spent = random.randint(30, 600)  # 30sec to 10min
                    actions = random.randint(0, 5)
                
                # Random date in last 30 days
                date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
                
                writer.writerow([session_id, user_id, user_type, page_views,
                               clicks, time_spent, actions, date])
        
        print(f"[SUCCESS] Created {filename}")
        self._print_distribution_stats(filename, "user_type")

    def generate_join_skew_scenario(self, large_table_size: int = 100000, 
                                   small_table_size: int = 1000) -> None:
        """Generate datasets that demonstrate join skew issues"""
        print(f"Generating join skew scenario data...")
        
        # Large table with skewed foreign keys
        large_filename = f"{self.output_dir}/large_table_skewed_fk.csv"
        small_filename = f"{self.output_dir}/small_dimension_table.csv"
        
        # First, create the small dimension table
        with open(small_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["dimension_id", "dimension_name", "category", "status"])
            
            for i in range(1, small_table_size + 1):
                writer.writerow([i, f"dimension_{i}", f"category_{i % 10}", 
                               random.choice(["active", "inactive"])])
        
        # Create large table with severely skewed foreign key distribution
        with open(large_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["fact_id", "dimension_id", "value", "amount", "created_date"])
            
            # Create extreme skew: 70% of records reference first 10 dimension IDs
            hot_dimensions = list(range(1, 11))
            cold_dimensions = list(range(11, small_table_size + 1))
            
            for i in range(large_table_size):
                fact_id = f"FACT_{i+1:06d}"
                
                # 70% go to hot dimensions, 30% to cold dimensions
                if random.random() < 0.7:
                    dimension_id = random.choice(hot_dimensions)
                else:
                    dimension_id = random.choice(cold_dimensions) if cold_dimensions else random.choice(hot_dimensions)
                
                value = random.randint(1, 1000)
                amount = round(random.uniform(10, 5000), 2)
                created_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
                
                writer.writerow([fact_id, dimension_id, value, amount, created_date])
        
        print(f"[SUCCESS] Created {large_filename}")
        print(f"[SUCCESS] Created {small_filename}")
        self._print_distribution_stats(large_filename, "dimension_id")

    def generate_aggregation_skew_data(self, total_records: int = 150000) -> None:
        """Generate data that causes skew during aggregation operations"""
        print(f"Generating aggregation skew data with {total_records} records...")
        
        filename = f"{self.output_dir}/aggregation_skewed_transactions.csv"
        
        # Create categories with different transaction volumes
        categories = {
            "popular_category": 0.6,    # 60% of transactions
            "medium_category_1": 0.15,  # 15% of transactions
            "medium_category_2": 0.15,  # 15% of transactions
            "rare_category_1": 0.05,    # 5% of transactions
            "rare_category_2": 0.05     # 5% of transactions
        }
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["transaction_id", "category", "subcategory", "amount", 
                           "merchant_id", "customer_id", "timestamp"])
            
            for i in range(total_records):
                transaction_id = f"TXN_{i+1:08d}"
                
                # Select category based on distribution
                rand = random.random()
                cumulative = 0
                selected_category = "popular_category"
                
                for category, probability in categories.items():
                    cumulative += probability
                    if rand <= cumulative:
                        selected_category = category
                        break
                
                subcategory = f"{selected_category}_sub_{random.randint(1, 5)}"
                amount = round(random.uniform(1, 1000), 2)
                merchant_id = f"MERCHANT_{random.randint(1, 5000)}"
                customer_id = f"CUSTOMER_{random.randint(1, 20000)}"
                
                # Random timestamp in last 6 months
                timestamp = (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d %H:%M:%S")
                
                writer.writerow([transaction_id, selected_category, subcategory, 
                               amount, merchant_id, customer_id, timestamp])
        
        print(f"[SUCCESS] Created {filename}")
        self._print_distribution_stats(filename, "category")

    def _print_distribution_stats(self, filename: str, column: str) -> None:
        """Print distribution statistics for a given column"""
        print(f"\n[STATS] Distribution stats for {column} in {filename}:")
        
        # Read and analyze distribution
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            values = [row[column] for row in reader]
        
        from collections import Counter
        distribution = Counter(values)
        total = len(values)
        
        # Show top 5 most frequent values
        top_5 = distribution.most_common(5)
        print(f"   Top 5 values:")
        for value, count in top_5:
            percentage = (count / total) * 100
            print(f"     {value}: {count} ({percentage:.1f}%)")
        
        # Calculate skew metrics
        max_count = max(distribution.values())
        avg_count = total / len(distribution)
        skew_ratio = max_count / avg_count
        
        print(f"   Skew ratio (max/avg): {skew_ratio:.2f}")
        if skew_ratio > 5:
            print("   [WARNING] HIGH SKEW detected!")
        elif skew_ratio > 2:
            print("   [WARNING] Moderate skew detected")

    def generate_all_skew_scenarios(self) -> None:
        """Generate all types of skewed datasets"""
        print("Generating comprehensive skewed datasets for testing...")
        print("=" * 60)
        
        self.generate_zipfian_skew_data(100000, 1.5)
        print()
        self.generate_temporal_skew_data(50000)
        print()
        self.generate_geographic_skew_data(75000)
        print()
        self.generate_user_behavior_skew_data(200000)
        print()
        self.generate_join_skew_scenario(100000, 1000)
        print()
        self.generate_aggregation_skew_data(150000)
        
        print("\n" + "=" * 60)
        print("[SUCCESS] All skewed datasets generated successfully!")
        print(f"[INFO] Files location: {self.output_dir}/")
        print("\nDatasets created:")
        print("  - zipfian_skewed_sales.csv (Product popularity skew)")
        print("  - temporal_skewed_events.csv (Time-based skew)")
        print("  - geographic_skewed_customers.csv (Location skew)")
        print("  - user_behavior_skewed.csv (User activity skew)")
        print("  - large_table_skewed_fk.csv (Join skew - large table)")
        print("  - small_dimension_table.csv (Join skew - dimension table)")
        print("  - aggregation_skewed_transactions.csv (Aggregation skew)")

def main():
    """Generate skewed datasets for testing"""
    print("Skewed Data Generator for PySpark Optimization Testing")
    print("=" * 60)
    
    generator = SkewedDataGenerator()
    
    print("\nChoose generation option:")
    print("1. Generate all skew scenarios")
    print("2. Zipfian distribution skew")
    print("3. Temporal skew")
    print("4. Geographic skew")
    print("5. User behavior skew")
    print("6. Join skew scenario")
    print("7. Aggregation skew")
    
    try:
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == "1":
            generator.generate_all_skew_scenarios()
        elif choice == "2":
            generator.generate_zipfian_skew_data()
        elif choice == "3":
            generator.generate_temporal_skew_data()
        elif choice == "4":
            generator.generate_geographic_skew_data()
        elif choice == "5":
            generator.generate_user_behavior_skew_data()
        elif choice == "6":
            generator.generate_join_skew_scenario()
        elif choice == "7":
            generator.generate_aggregation_skew_data()
        else:
            print("Invalid choice")
            
    except KeyboardInterrupt:
        print("\nData generation stopped.")

if __name__ == "__main__":
    main()
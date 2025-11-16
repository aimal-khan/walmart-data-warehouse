import mysql.connector
import pandas as pd
import threading
import time
from collections import deque, defaultdict
from datetime import datetime
import queue

class QueueNode:
    """Node for doubly-linked list queue"""
    def __init__(self, key, hash_entries):
        self.key = key
        self.hash_entries = hash_entries  # List of hash table indices for this key
        self.prev = None
        self.next = None

class DoublyLinkedQueue:
    """Doubly-linked list for FIFO queue with random deletion"""
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0
    
    def enqueue(self, key, hash_entries):
        node = QueueNode(key, hash_entries)
        if self.tail is None:
            self.head = self.tail = node
        else:
            self.tail.next = node
            node.prev = self.tail
            self.tail = node
        self.size += 1
        return node
    
    def dequeue(self):
        if self.head is None:
            return None
        node = self.head
        self.head = node.next
        if self.head:
            self.head.prev = None
        else:
            self.tail = None
        self.size -= 1
        return node
    
    def remove_node(self, node):
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev
        self.size -= 1
    
    def is_empty(self):
        return self.head is None
    
    def peek(self):
        return self.head.key if self.head else None

class HybridJoin:
    def __init__(self, db_config, hs=10000, vp=500):
        self.db_config = db_config
        self.hs = hs  # Hash table size
        self.vp = vp  # Disk partition size
        self.w = hs   # Available slots
        
        # Data structures
        self.hash_table = defaultdict(list)  # Multi-map: key -> [(tuple, queue_node), ...]
        self.queue = DoublyLinkedQueue()
        self.stream_buffer = queue.Queue()
        self.disk_buffer = []
        
        # Threading control
        self.running = True
        self.lock = threading.Lock()
        
        # Statistics
        self.processed_tuples = 0
        self.joined_tuples = 0
        
    def hash_function(self, key):
        """Hash function to map join key to slot"""
        return hash(key) % self.hs
    
    def load_master_data(self):
        """Load master data (customer and product) into memory"""
        print("Loading master data...")
        
        # Load customer master data
        self.customer_md = pd.read_csv('customer_master_data.csv')
        self.customer_md['Customer_ID'] = self.customer_md['Customer_ID'].astype(str).str.strip()
        self.customer_dict = self.customer_md.set_index('Customer_ID').to_dict('index')
        
        # Load product master data
        self.product_md = pd.read_csv('product_master_data.csv')
        self.product_md['Product_ID'] = self.product_md['Product_ID'].astype(str).str.strip()
        self.product_dict = self.product_md.set_index('Product_ID').to_dict('index')
        
        print(f"Loaded {len(self.customer_dict)} customers and {len(self.product_dict)} products")
    
    def stream_producer(self):
        """Thread to continuously read transactional data and add to stream buffer"""
        print("Stream producer started...")
        
        try:
            df = pd.read_csv('transactional_data.csv')
            df['Customer_ID'] = df['Customer_ID'].astype(str).str.strip()
            df['Product_ID'] = df['Product_ID'].astype(str).str.strip()
            df['orderID'] = df['orderID'].astype(str).str.strip()
            
            for idx, row in df.iterrows():
                if not self.running:
                    break
                
                tuple_data = {
                    'orderID': row['orderID'],
                    'Customer_ID': row['Customer_ID'],
                    'Product_ID': row['Product_ID'],
                    'quantity': row['quantity'],
                    'date': row['date']
                }
                
                self.stream_buffer.put(tuple_data)
                
                # Simulate streaming with small delay
                if idx % 100 == 0:
                    time.sleep(0.01)
                    print(f"Stream producer: {idx} tuples added to buffer")
            
            print("Stream producer finished reading all data")
        except Exception as e:
            print(f"Error in stream producer: {e}")
    
    def populate_dimension_tables(self, conn):
        """Populate dimension tables from master data"""
        cursor = conn.cursor()
        
        print("Populating dimension tables...")
        
        # Populate dim_customer
        for cust_id, cust_data in self.customer_dict.items():
            cursor.execute("""
                INSERT IGNORE INTO dim_customer 
                (Customer_ID, Gender, Age, Occupation, City_Category, 
                 Stay_In_Current_City_Years, Marital_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (cust_id, cust_data['Gender'], cust_data['Age'], 
                  cust_data['Occupation'], cust_data['City_Category'],
                  cust_data['Stay_In_Current_City_Years'], cust_data['Marital_Status']))
        
        # Populate dim_store and dim_supplier first
        stores = self.product_md[['storeID', 'storeName']].drop_duplicates()
        for _, store in stores.iterrows():
            cursor.execute("""
                INSERT IGNORE INTO dim_store (storeID, storeName)
                VALUES (%s, %s)
            """, (store['storeID'], store['storeName']))
        
        suppliers = self.product_md[['supplierID', 'supplierName']].drop_duplicates()
        for _, supplier in suppliers.iterrows():
            cursor.execute("""
                INSERT IGNORE INTO dim_supplier (supplierID, supplierName)
                VALUES (%s, %s)
            """, (supplier['supplierID'], supplier['supplierName']))
        
        conn.commit()
        
        # Populate dim_product with foreign keys
        for prod_id, prod_data in self.product_dict.items():
            # Get store_key
            cursor.execute("SELECT store_key FROM dim_store WHERE storeID = %s", 
                         (prod_data['storeID'],))
            store_key = cursor.fetchone()[0]
            
            # Get supplier_key
            cursor.execute("SELECT supplier_key FROM dim_supplier WHERE supplierID = %s",
                         (prod_data['supplierID'],))
            supplier_key = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT IGNORE INTO dim_product 
                (Product_ID, Product_Category, price, store_key, supplier_key)
                VALUES (%s, %s, %s, %s, %s)
            """, (prod_id, prod_data['Product_Category'], 
                  prod_data['price$'], store_key, supplier_key))
        
        conn.commit()
        print("Dimension tables populated")
    
    def populate_date_dimension(self, conn, dates):
        """Populate date dimension for unique dates"""
        cursor = conn.cursor()
        
        for date_str in dates:
            try:
                date_obj = pd.to_datetime(date_str)
                
                # Determine season
                month = date_obj.month
                if month in [3, 4, 5]:
                    season = 'Spring'
                elif month in [6, 7, 8]:
                    season = 'Summer'
                elif month in [9, 10, 11]:
                    season = 'Fall'
                else:
                    season = 'Winter'
                
                cursor.execute("""
                    INSERT IGNORE INTO dim_date 
                    (full_date, day_of_week, day_of_month, month, month_name, 
                     quarter, year, is_weekend, season)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    date_obj.date(),
                    date_obj.strftime('%A'),
                    date_obj.day,
                    date_obj.month,
                    date_obj.strftime('%B'),
                    (date_obj.month - 1) // 3 + 1,
                    date_obj.year,
                    date_obj.weekday() >= 5,
                    season
                ))
            except Exception as e:
                print(f"Error processing date {date_str}: {e}")
        
        conn.commit()
    
    def join_processor(self):
        """Thread to implement HYBRIDJOIN algorithm"""
        print("Join processor started...")
        
        try:
            conn = mysql.connector.connect(**self.db_config)
            
            # Populate dimension tables
            self.populate_dimension_tables(conn)
            
            # Collect all unique dates first
            all_dates = set()
            temp_stream = []
            
            while not self.stream_buffer.empty():
                tuple_data = self.stream_buffer.get()
                temp_stream.append(tuple_data)
                all_dates.add(tuple_data['date'])
            
            # Populate date dimension
            self.populate_date_dimension(conn, all_dates)
            
            # Put data back in buffer
            for t in temp_stream:
                self.stream_buffer.put(t)
            
            # Main HYBRIDJOIN loop
            while self.running or not self.stream_buffer.empty():
                
                # Step 1-2: Load stream tuples into hash table
                with self.lock:
                    loaded = 0
                    while loaded < self.w and not self.stream_buffer.empty():
                        try:
                            stream_tuple = self.stream_buffer.get_nowait()
                            
                            # Hash on Customer_ID (join key)
                            join_key = stream_tuple['Customer_ID']
                            slot = self.hash_function(join_key)
                            
                            # Create queue node and add to hash table
                            hash_entry_idx = len(self.hash_table[slot])
                            queue_node = self.queue.enqueue(join_key, [(slot, hash_entry_idx)])
                            
                            self.hash_table[slot].append((stream_tuple, queue_node))
                            loaded += 1
                            
                        except queue.Empty:
                            break
                    
                    self.w = 0  # Reset available slots
                
                # Step 3: Get oldest key from queue and load disk partition
                if self.queue.is_empty():
                    if self.stream_buffer.empty():
                        time.sleep(0.1)
                    continue
                
                oldest_key = self.queue.peek()
                
                # Load matching customer data (simulating disk partition)
                if oldest_key in self.customer_dict:
                    self.disk_buffer = [self.customer_dict[oldest_key]]
                else:
                    self.disk_buffer = []
                
                # Step 4: Probe hash table with disk buffer
                matched_nodes = []
                
                for disk_tuple in self.disk_buffer:
                    slot = self.hash_function(oldest_key)
                    
                    if slot in self.hash_table:
                        for i, (stream_tuple, queue_node) in enumerate(self.hash_table[slot]):
                            if stream_tuple['Customer_ID'] == oldest_key:
                                # Join match found - enrich and insert into DW
                                self.insert_to_dw(conn, stream_tuple, disk_tuple)
                                
                                matched_nodes.append((slot, i, queue_node))
                                self.joined_tuples += 1
                
                # Remove matched tuples from hash table and queue
                with self.lock:
                    for slot, idx, queue_node in sorted(matched_nodes, 
                                                        key=lambda x: (x[0], x[1]), 
                                                        reverse=True):
                        if slot in self.hash_table and idx < len(self.hash_table[slot]):
                            del self.hash_table[slot][idx]
                            self.queue.remove_node(queue_node)
                            self.w += 1
                
                self.processed_tuples += len(matched_nodes)
                
                if self.processed_tuples % 100 == 0:
                    print(f"Processed: {self.processed_tuples}, Joined: {self.joined_tuples}")
            
            conn.close()
            print("Join processor finished")
            
        except Exception as e:
            print(f"Error in join processor: {e}")
            import traceback
            traceback.print_exc()
    
    def insert_to_dw(self, conn, stream_tuple, customer_data):
        """Insert enriched data into data warehouse"""
        cursor = conn.cursor()
        
        try:
            # Get customer_key
            cursor.execute("""
                SELECT customer_key FROM dim_customer 
                WHERE Customer_ID = %s
            """, (stream_tuple['Customer_ID'],))
            result = cursor.fetchone()
            if not result:
                return
            customer_key = result[0]
            
            # Get product_key
            cursor.execute("""
                SELECT product_key FROM dim_product 
                WHERE Product_ID = %s
            """, (stream_tuple['Product_ID'],))
            result = cursor.fetchone()
            if not result:
                return
            product_key = result[0]
            
            # Get price for total_amount calculation
            cursor.execute("""
                SELECT price FROM dim_product 
                WHERE product_key = %s
            """, (product_key,))
            price = cursor.fetchone()[0]
            
            # Get date_key
            cursor.execute("""
                SELECT date_key FROM dim_date 
                WHERE full_date = %s
            """, (pd.to_datetime(stream_tuple['date']).date(),))
            result = cursor.fetchone()
            if not result:
                return
            date_key = result[0]
            
            # Calculate total_amount
            total_amount = stream_tuple['quantity'] * price
            
            # Insert into fact table
            cursor.execute("""
                INSERT INTO fact_sales 
                (orderID, customer_key, product_key, date_key, quantity, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (stream_tuple['orderID'], customer_key, product_key, 
                  date_key, stream_tuple['quantity'], total_amount))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error inserting to DW: {e}")
            conn.rollback()
    
    def run(self):
        """Start the HYBRIDJOIN process"""
        # Load master data
        self.load_master_data()
        
        # Start threads
        producer_thread = threading.Thread(target=self.stream_producer)
        processor_thread = threading.Thread(target=self.join_processor)
        
        producer_thread.start()
        time.sleep(1)  # Let producer start first
        processor_thread.start()
        
        # Wait for completion
        producer_thread.join()
        print("Producer finished, waiting for processor...")
        
        # Wait for queue to be empty
        while not self.stream_buffer.empty() or not self.queue.is_empty():
            time.sleep(0.5)
        
        self.running = False
        processor_thread.join()
        
        print(f"\nHYBRIDJOIN Complete!")
        print(f"Total tuples processed: {self.processed_tuples}")
        print(f"Total tuples joined: {self.joined_tuples}")

def main():
    print("=" * 60)
    print("HYBRIDJOIN Algorithm - Walmart Data Warehouse")
    print("=" * 60)
    
    # Get database credentials
    print("\nEnter Database Credentials:")
    host = input("Host (default: localhost): ").strip() or "localhost"
    user = input("Username: ").strip()
    password = input("Password: ").strip()
    database = input("Database name: ").strip()
    
    db_config = {
        'host': host,
        'user': user,
        'password': password,
        'database': database
    }
    
    # Test connection
    try:
        conn = mysql.connector.connect(**db_config)
        print("\n✓ Database connection successful!")
        conn.close()
    except Exception as e:
        print(f"\n✗ Database connection failed: {e}")
        return
    
    # Initialize and run HYBRIDJOIN
    print("\nStarting HYBRIDJOIN process...")
    hybrid_join = HybridJoin(db_config)
    hybrid_join.run()
    
    print("\n" + "=" * 60)
    print("Data loading complete! Data warehouse is ready for analysis.")
    print("=" * 60)

if __name__ == "__main__":
    main()
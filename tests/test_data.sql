-- Comprehensive test data for MindsDB PostgreSQL handler testing
-- This file sets up a complete e-commerce-like database with multiple schemas

-- Create schemas
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS inventory;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- PUBLIC SCHEMA TABLES
-- =============================================================================

-- Customers table with comprehensive customer data
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    customer_type VARCHAR(20) DEFAULT 'regular'
);

-- Products table with pricing and categorization
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    sku VARCHAR(50) UNIQUE,
    description TEXT,
    weight DECIMAL(8,2),
    dimensions VARCHAR(50),
    color VARCHAR(30),
    brand VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Orders table with comprehensive order information
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    ship_date DATE,
    required_date DATE,
    order_status VARCHAR(20) DEFAULT 'pending',
    total_amount DECIMAL(12,2),
    shipping_cost DECIMAL(8,2) DEFAULT 0.00,
    tax_amount DECIMAL(10,2) DEFAULT 0.00,
    discount_amount DECIMAL(10,2) DEFAULT 0.00,
    payment_method VARCHAR(30),
    shipping_address TEXT,
    notes TEXT
);

-- Order items for detailed line items
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0.00,
    line_total DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount_percent/100)) STORED
);

-- Customer addresses for multiple shipping locations
CREATE TABLE IF NOT EXISTS customer_addresses (
    address_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    address_type VARCHAR(20) DEFAULT 'shipping', -- shipping, billing, both
    street_address TEXT NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(2) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    country VARCHAR(50) DEFAULT 'USA',
    is_default BOOLEAN DEFAULT FALSE
);

-- Simple test table (keeping your original)
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    value TEXT
);

-- =============================================================================
-- SALES SCHEMA TABLES
-- =============================================================================

-- Sales representatives
CREATE TABLE IF NOT EXISTS sales.sales_reps (
    rep_id SERIAL PRIMARY KEY,
    employee_id VARCHAR(20) UNIQUE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    hire_date DATE NOT NULL,
    territory VARCHAR(50),
    commission_rate DECIMAL(5,4) DEFAULT 0.0500,
    manager_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE
);

-- Sales territories
CREATE TABLE IF NOT EXISTS sales.territories (
    territory_id SERIAL PRIMARY KEY,
    territory_name VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    country VARCHAR(50) DEFAULT 'USA',
    rep_id INTEGER REFERENCES sales.sales_reps(rep_id)
);

-- Sales targets
CREATE TABLE IF NOT EXISTS sales.sales_targets (
    target_id SERIAL PRIMARY KEY,
    rep_id INTEGER REFERENCES sales.sales_reps(rep_id),
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    target_amount DECIMAL(12,2) NOT NULL,
    actual_amount DECIMAL(12,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INVENTORY SCHEMA TABLES
-- =============================================================================

-- Warehouses
CREATE TABLE IF NOT EXISTS inventory.warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_code VARCHAR(10) UNIQUE NOT NULL,
    warehouse_name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    address TEXT,
    capacity INTEGER,
    manager_name VARCHAR(100),
    phone VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE
);

-- Stock levels
CREATE TABLE IF NOT EXISTS inventory.stock_levels (
    stock_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    warehouse_id INTEGER REFERENCES inventory.warehouses(warehouse_id),
    quantity_on_hand INTEGER DEFAULT 0,
    quantity_reserved INTEGER DEFAULT 0,
    quantity_available GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_level INTEGER DEFAULT 10,
    max_stock_level INTEGER DEFAULT 1000,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_count_date DATE
);

-- Inventory movements for tracking stock changes
CREATE TABLE IF NOT EXISTS inventory.inventory_movements (
    movement_id SERIAL PRIMARY KEY,
    product_id INTEGER,
    warehouse_id INTEGER REFERENCES inventory.warehouses(warehouse_id),
    movement_type VARCHAR(20), -- 'in', 'out', 'adjustment', 'transfer'
    quantity INTEGER NOT NULL,
    reference_id INTEGER, -- order_id, transfer_id, etc.
    reference_type VARCHAR(20), -- 'order', 'transfer', 'adjustment'
    movement_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- =============================================================================
-- ANALYTICS SCHEMA VIEWS
-- =============================================================================

-- Customer summary view
CREATE OR REPLACE VIEW analytics.customer_summary AS
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as full_name,
    c.email,
    c.city,
    c.state,
    c.customer_type,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.total_amount), 0) as total_spent,
    COALESCE(AVG(o.total_amount), 0) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    CASE 
        WHEN COUNT(o.order_id) = 0 THEN 'No Orders'
        WHEN COUNT(o.order_id) = 1 THEN 'One-time'
        WHEN COUNT(o.order_id) <= 5 THEN 'Regular'
        ELSE 'Frequent'
    END as customer_segment,
    c.created_at
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state, c.customer_type, c.created_at;

-- Product performance view
CREATE OR REPLACE VIEW analytics.product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.cost,
    (p.price - p.cost) as profit_per_unit,
    ROUND((p.price - p.cost) / p.price * 100, 2) as profit_margin_percent,
    COUNT(oi.item_id) as times_ordered,
    COALESCE(SUM(oi.quantity), 0) as total_quantity_sold,
    COALESCE(SUM(oi.line_total), 0) as total_revenue,
    COALESCE(SUM(oi.quantity * p.cost), 0) as total_cost,
    COALESCE(SUM(oi.line_total) - SUM(oi.quantity * p.cost), 0) as total_profit
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.order_id AND o.order_status IN ('shipped', 'delivered')
GROUP BY p.product_id, p.product_name, p.category, p.brand, p.price, p.cost;

-- Monthly sales summary view
CREATE OR REPLACE VIEW analytics.monthly_sales AS
SELECT 
    EXTRACT(YEAR FROM o.order_date) as year,
    EXTRACT(MONTH FROM o.order_date) as month,
    COUNT(DISTINCT o.order_id) as total_orders,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    SUM(oi.quantity) as total_items_sold
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_status IN ('shipped', 'delivered')
GROUP BY EXTRACT(YEAR FROM o.order_date), EXTRACT(MONTH FROM o.order_date)
ORDER BY year, month;

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

-- Insert customers with diverse profiles
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, customer_type) VALUES
('John', 'Doe', 'john.doe@email.com', '555-0101', '123 Main St', 'New York', 'NY', '10001', 'premium'),
('Jane', 'Smith', 'jane.smith@email.com', '555-0102', '456 Oak Ave', 'Los Angeles', 'CA', '90001', 'regular'),
('Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine St', 'Chicago', 'IL', '60601', 'regular'),
('Alice', 'Williams', 'alice.williams@email.com', '555-0104', '321 Elm Dr', 'Houston', 'TX', '77001', 'premium'),
('Charlie', 'Brown', 'charlie.brown@email.com', '555-0105', '654 Maple Ln', 'Phoenix', 'AZ', '85001', 'regular'),
('Diana', 'Miller', 'diana.miller@email.com', '555-0106', '987 Cedar St', 'Philadelphia', 'PA', '19101', 'premium'),
('Edward', 'Davis', 'edward.davis@email.com', '555-0107', '147 Birch Ave', 'San Antonio', 'TX', '78201', 'regular'),
('Fiona', 'Wilson', 'fiona.wilson@email.com', '555-0108', '258 Walnut St', 'San Diego', 'CA', '92101', 'regular'),
('George', 'Taylor', 'george.taylor@email.com', '555-0109', '369 Oak St', 'Miami', 'FL', '33101', 'premium'),
('Helen', 'Anderson', 'helen.anderson@email.com', '555-0110', '741 Pine Ave', 'Seattle', 'WA', '98101', 'regular')
ON CONFLICT (email) DO NOTHING;

-- Insert products with variety
INSERT INTO products (product_name, category, price, cost, sku, description, weight, brand, color) VALUES
('MacBook Pro 16"', 'Electronics', 2499.99, 1800.00, 'LAP001', 'High-performance laptop for professionals', 4.7, 'Apple', 'Space Gray'),
('Wireless Mouse', 'Electronics', 29.99, 15.00, 'MOU001', 'Ergonomic wireless mouse with USB receiver', 0.3, 'Logitech', 'Black'),
('Mechanical Keyboard', 'Electronics', 129.99, 65.00, 'KEY001', 'RGB backlit mechanical gaming keyboard', 1.2, 'Razer', 'Black'),
('Office Chair', 'Furniture', 299.99, 180.00, 'CHR001', 'Ergonomic office chair with lumbar support', 25.0, 'Herman Miller', 'Black'),
('Standing Desk', 'Furniture', 599.99, 350.00, 'DSK001', 'Height-adjustable standing desk', 45.0, 'UPLIFT', 'Bamboo'),
('Desk Lamp', 'Furniture', 79.99, 35.00, 'LAM001', 'LED desk lamp with adjustable brightness', 2.8, 'BenQ', 'White'),
('Coffee Mug', 'Office Supplies', 12.99, 5.00, 'MUG001', 'Ceramic coffee mug with company logo', 0.8, 'Generic', 'White'),
('Notebook', 'Office Supplies', 4.99, 2.00, 'NOT001', 'Spiral-bound notebook, 200 pages', 0.5, 'Moleskine', 'Black'),
('Pen Set', 'Office Supplies', 24.99, 10.00, 'PEN001', 'Set of 5 premium ballpoint pens', 0.3, 'Parker', 'Blue'),
('Monitor 27"', 'Electronics', 329.99, 200.00, 'MON001', '4K UHD monitor with USB-C connectivity', 8.5, 'Dell', 'Black'),
('Webcam HD', 'Electronics', 89.99, 45.00, 'CAM001', '1080p HD webcam with built-in microphone', 0.4, 'Logitech', 'Black'),
('Headphones', 'Electronics', 199.99, 120.00, 'HEA001', 'Noise-cancelling over-ear headphones', 0.7, 'Sony', 'Black')
ON CONFLICT (sku) DO NOTHING;

-- Insert orders with realistic patterns
INSERT INTO orders (customer_id, order_date, ship_date, required_date, order_status, total_amount, shipping_cost, tax_amount, payment_method) VALUES
(1, '2024-01-15', '2024-01-17', '2024-01-20', 'delivered', 2579.97, 0.00, 154.80, 'credit_card'),
(2, '2024-01-20', '2024-01-22', '2024-01-25', 'delivered', 369.97, 15.00, 22.20, 'paypal'),
(1, '2024-02-01', '2024-02-03', '2024-02-05', 'delivered', 92.98, 8.00, 5.58, 'credit_card'),
(3, '2024-02-05', NULL, '2024-02-10', 'pending', 599.99, 0.00, 36.00, 'bank_transfer'),
(4, '2024-02-10', '2024-02-12', '2024-02-15', 'shipped', 42.97, 5.00, 2.58, 'credit_card'),
(5, '2024-02-15', '2024-02-17', '2024-02-20', 'delivered', 159.98, 10.00, 9.60, 'debit_card'),
(6, '2024-02-20', '2024-02-22', '2024-02-25', 'delivered', 749.98, 0.00, 45.00, 'credit_card'),
(2, '2024-02-25', '2024-02-27', '2024-03-01', 'shipped', 224.98, 12.00, 13.50, 'paypal'),
(7, '2024-03-01', NULL, '2024-03-05', 'processing', 329.99, 15.00, 19.80, 'credit_card'),
(8, '2024-03-05', '2024-03-07', '2024-03-10', 'delivered', 89.99, 8.00, 5.40, 'debit_card'),
(9, '2024-03-10', '2024-03-12', '2024-03-15', 'delivered', 1299.99, 0.00, 78.00, 'credit_card'),
(10, '2024-03-15', '2024-03-17', '2024-03-20', 'shipped', 199.98, 10.00, 12.00, 'paypal'),
(1, '2024-03-20', '2024-03-22', '2024-03-25', 'delivered', 449.97, 15.00, 27.00, 'credit_card'),
(4, '2024-03-25', NULL, '2024-03-30', 'pending', 179.99, 8.00, 10.80, 'credit_card'),
(6, '2024-04-01', '2024-04-03', '2024-04-05', 'delivered', 299.99, 12.00, 18.00, 'debit_card')
ON CONFLICT DO NOTHING;

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent) VALUES
-- Order 1: Premium customer big purchase
(1, 1, 1, 2499.99, 0.00),  -- MacBook Pro
(1, 2, 1, 29.99, 0.00),    -- Wireless Mouse
(1, 3, 1, 129.99, 10.00),  -- Keyboard with discount

-- Order 2: Regular purchase
(2, 4, 1, 299.99, 0.00),   -- Office Chair
(2, 6, 1, 79.99, 0.00),    -- Desk Lamp

-- Order 3: Small order
(3, 7, 1, 12.99, 0.00),    -- Coffee Mug
(3, 8, 2, 4.99, 0.00),     -- Notebooks
(3, 9, 3, 24.99, 15.00),   -- Pen Sets with bulk discount

-- Order 4: Furniture order
(4, 5, 1, 599.99, 0.00),   -- Standing Desk

-- Order 5: Office supplies
(5, 7, 2, 12.99, 0.00),    -- Coffee Mugs
(5, 8, 1, 4.99, 0.00),     -- Notebook
(5, 9, 1, 24.99, 0.00),    -- Pen Set

-- Order 6: Electronics bundle
(6, 10, 1, 329.99, 0.00),  -- Monitor
(6, 11, 1, 89.99, 0.00),   -- Webcam
(6, 12, 1, 199.99, 5.00),  -- Headphones with small discount
(6, 3, 1, 129.99, 0.00),   -- Keyboard

-- Order 7: Accessories
(7, 2, 1, 29.99, 0.00),    -- Wireless Mouse
(7, 12, 1, 199.99, 0.00),  -- Headphones

-- Order 8: Monitor only
(8, 10, 1, 329.99, 0.00),  -- Monitor

-- Order 9: Webcam only  
(9, 11, 1, 89.99, 0.00),   -- Webcam

-- Order 10: Simple supplies
(10, 7, 1, 12.99, 0.00),   -- Coffee Mug
(10, 8, 2, 4.99, 0.00),    -- Notebooks

-- Order 11: Premium purchase
(11, 1, 1, 2499.99, 5.00), -- MacBook Pro with discount
(11, 10, 1, 329.99, 0.00), -- Monitor
(11, 3, 1, 129.99, 0.00),  -- Keyboard

-- Order 12: Mixed order
(12, 2, 2, 29.99, 0.00),   -- Wireless Mice
(12, 6, 1, 79.99, 10.00),  -- Desk Lamp with discount
(12, 9, 2, 24.99, 0.00),   -- Pen Sets

-- Order 13: Electronics
(13, 12, 1, 199.99, 0.00), -- Headphones
(13, 11, 1, 89.99, 15.00), -- Webcam with discount
(13, 2, 1, 29.99, 0.00),   -- Wireless Mouse

-- Order 14: Furniture
(14, 4, 1, 299.99, 0.00),  -- Office Chair

-- Order 15: Office setup
(15, 5, 1, 599.99, 0.00),  -- Standing Desk
(15, 6, 1, 79.99, 0.00),   -- Desk Lamp
ON CONFLICT DO NOTHING;

-- Insert customer addresses
INSERT INTO customer_addresses (customer_id, address_type, street_address, city, state, zip_code, is_default) VALUES
(1, 'billing', '123 Main St', 'New York', 'NY', '10001', TRUE),
(1, 'shipping', '456 Work Ave', 'New York', 'NY', '10002', FALSE),
(2, 'both', '456 Oak Ave', 'Los Angeles', 'CA', '90001', TRUE),
(3, 'both', '789 Pine St', 'Chicago', 'IL', '60601', TRUE),
(4, 'billing', '321 Elm Dr', 'Houston', 'TX', '77001', TRUE),
(4, 'shipping', '654 Office Blvd', 'Houston', 'TX', '77002', FALSE),
(5, 'both', '654 Maple Ln', 'Phoenix', 'AZ', '85001', TRUE),
(6, 'billing', '987 Cedar St', 'Philadelphia', 'PA', '19101', TRUE),
(7, 'both', '147 Birch Ave', 'San Antonio', 'TX', '78201', TRUE),
(8, 'both', '258 Walnut St', 'San Diego', 'CA', '92101', TRUE),
(9, 'billing', '369 Oak St', 'Miami', 'FL', '33101', TRUE),
(9, 'shipping', '741 Beach Blvd', 'Miami', 'FL', '33102', FALSE),
(10, 'both', '741 Pine Ave', 'Seattle', 'WA', '98101', TRUE)
ON CONFLICT DO NOTHING;

-- Insert sales representatives
INSERT INTO sales.sales_reps (employee_id, first_name, last_name, email, hire_date, territory, commission_rate, is_active) VALUES
('EMP001', 'Mike', 'Sales', 'mike.sales@company.com', '2023-01-15', 'West Coast', 0.0750, TRUE),
('EMP002', 'Sarah', 'Closer', 'sarah.closer@company.com', '2023-03-01', 'East Coast', 0.0600, TRUE),
('EMP003', 'Tom', 'Hunter', 'tom.hunter@company.com', '2023-06-15', 'Midwest', 0.0550, TRUE),
('EMP004', 'Lisa', 'Champion', 'lisa.champion@company.com', '2023-09-01', 'South', 0.0650, TRUE),
('EMP005', 'David', 'Achiever', 'david.achiever@company.com', '2023-12-01', 'Northeast', 0.0700, TRUE)
ON CONFLICT (employee_id) DO NOTHING;

-- Insert territories
INSERT INTO sales.territories (territory_name, region, country, rep_id) VALUES
('California', 'West Coast', 'USA', 1),
('Oregon/Washington', 'West Coast', 'USA', 1),
('New York/New Jersey', 'East Coast', 'USA', 2),
('Florida/Georgia', 'East Coast', 'USA', 2),
('Illinois/Wisconsin', 'Midwest', 'USA', 3),
('Ohio/Michigan', 'Midwest', 'USA', 3),
('Texas', 'South', 'USA', 4),
('North Carolina/South Carolina', 'South', 'USA', 4),
('Massachusetts/Connecticut', 'Northeast', 'USA', 5),
('Pennsylvania/Delaware', 'Northeast', 'USA', 5)
ON CONFLICT DO NOTHING;

-- Insert sales targets
INSERT INTO sales.sales_targets (rep_id, year, quarter, target_amount, actual_amount) VALUES
(1, 2024, 1, 250000.00, 280000.00),
(1, 2024, 2, 275000.00, 0.00),
(2, 2024, 1, 200000.00, 220000.00),
(2, 2024, 2, 225000.00, 0.00),
(3, 2024, 1, 175000.00, 165000.00),
(3, 2024, 2, 200000.00, 0.00),
(4, 2024, 1, 300000.00, 325000.00),
(4, 2024, 2, 350000.00, 0.00),
(5, 2024, 1, 180000.00, 190000.00),
(5, 2024, 2, 200000.00, 0.00)
ON CONFLICT DO NOTHING;

-- Insert warehouses
INSERT INTO inventory.warehouses (warehouse_code, warehouse_name, location, address, capacity, manager_name, phone, is_active) VALUES
('WH001', 'Main Distribution Center', 'Denver, CO', '1000 Industrial Blvd, Denver, CO 80202', 50000, 'Robert Wilson', '303-555-0001', TRUE),
('WH002', 'East Coast Hub', 'Atlanta, GA', '2000 Logistics Way, Atlanta, GA 30309', 35000, 'Jennifer Martinez', '404-555-0002', TRUE),
('WH003', 'West Coast Hub', 'Los Angeles, CA', '3000 Shipping Ave, Los Angeles, CA 90021', 40000, 'Michael Chen', '213-555-0003', TRUE),
('WH004', 'Midwest Center', 'Chicago, IL', '4000 Distribution Dr, Chicago, IL 60607', 30000, 'Sarah Johnson', '312-555-0004', TRUE),
('WH005', 'Southwest Hub', 'Dallas, TX', '5000 Freight St, Dallas, TX 75207', 25000, 'Carlos Rodriguez', '214-555-0005', TRUE)
ON CONFLICT (warehouse_code) DO NOTHING;

-- Insert stock levels
INSERT INTO inventory.stock_levels (product_id, warehouse_id, quantity_on_hand, quantity_reserved, reorder_level, max_stock_level, last_count_date) VALUES
-- MacBook Pro distribution
(1, 1, 45, 5, 10, 100, '2024-03-01'),
(1, 2, 25, 3, 8, 60, '2024-03-01'),
(1, 3, 35, 7, 12, 80, '2024-03-01'),
(1, 4, 20, 2, 5, 50, '2024-03-01'),
(1, 5, 15, 1, 5, 40, '2024-03-01'),

-- Accessories in all warehouses
(2, 1, 200, 20, 50, 500, '2024-03-01'),
(2, 2, 150, 15, 40, 400, '2024-03-01'),
(2, 3, 180, 25, 45, 450, '2024-03-01'),
(2, 4, 120, 10, 30, 300, '2024-03-01'),
(2, 5, 100, 8, 25, 250, '2024-03-01'),

-- Keyboards
(3, 1, 75, 10, 20, 200, '2024-03-01'),
(3, 2, 50, 5, 15, 150, '2024-03-01'),
(3, 3, 60, 8, 18, 180, '2024-03-01'),

-- Furniture (mainly in main warehouse)
(4, 1, 30, 3, 8, 80, '2024-03-01'),
(4, 2, 15, 2, 5, 40, '2024-03-01'),
(5, 1, 20, 1, 5, 50, '2024-03-01'),
(5, 2, 10, 0, 3, 25, '2024-03-01'),

-- Office supplies distributed
(7, 1, 500, 50, 100, 1000, '2024-03-01'),
(7, 2, 300, 30, 75, 600, '2024-03-01'),
(7, 3, 400, 40, 85, 800, '2024-03-01'),
(8, 1, 1000, 100, 200, 2000, '2024-03-01'),
(8, 2, 750, 75, 150, 1500, '2024-03-01'),
(8, 3, 900, 90, 180, 1800, '2024-03-01')
ON CONFLICT DO NOTHING;

-- Insert some inventory movements
INSERT INTO inventory.inventory_movements (product_id, warehouse_id, movement_type, quantity, reference_id, reference_type, movement_date, notes) VALUES
(1, 1, 'out', -1, 1, 'order', '2024-01-15 10:00:00', 'Order fulfillment'),
(2, 1, 'out', -1, 1, 'order', '2024-01-15 10:00:00', 'Order fulfillment'),
(3, 1, 'out', -1, 1, 'order', '2024-01-15 10:00:00', 'Order fulfillment'),
(4, 2, 'out', -1, 2, 'order', '2024-01-20 14:30:00', 'Order fulfillment'),
(6, 2, 'out', -1, 2, 'order', '2024-01-20 14:30:00', 'Order fulfillment'),
(7, 1, 'in', 100, NULL, 'receiving', '2024-02-01 09:00:00', 'New stock received'),
(8, 1, 'in', 200, NULL, 'receiving', '2024-02-01 09:00:00', 'New stock received'),
(1, 1, 'in', 10, NULL, 'receiving', '2024-02-15 11:00:00', 'Restock from vendor')
ON CONFLICT DO NOTHING;

-- Insert simple test data (keeping your original)
INSERT INTO test_table (value) VALUES 
('text1'), ('text2'), ('text3'), ('sample_data'), ('test_row')
ON CONFLICT DO NOTHING;
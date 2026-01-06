-- Create ecommerce database schema
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Customers table
CREATE TABLE ecommerce.customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES ecommerce.customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    total_amount DECIMAL(10, 2),
    shipping_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Items table
CREATE TABLE ecommerce.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES ecommerce.orders(order_id),
    product_id INT REFERENCES ecommerce.products(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2),
    subtotal DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO ecommerce.customers (email, first_name, last_name, phone) VALUES
('john.doe@email.com', 'John', 'Doe', '+1234567890'),
('jane.smith@email.com', 'Jane', 'Smith', '+1234567891'),
('bob.johnson@email.com', 'Bob', 'Johnson', '+1234567892');

INSERT INTO ecommerce.products (product_name, category, price, stock_quantity) VALUES
('Laptop Pro 15', 'Electronics', 1299.99, 50),
('Wireless Mouse', 'Electronics', 29.99, 200),
('Office Chair', 'Furniture', 199.99, 75),
('USB-C Cable', 'Accessories', 12.99, 500);


-- Create publication for Debezium
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA ecommerce TO postgres;
GRANT USAGE ON SCHEMA ecommerce TO postgres;
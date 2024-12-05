-- Device Table
CREATE TABLE device (
    device_id VARCHAR(50) PRIMARY KEY,
    model_id VARCHAR(50),
    battery_voltage FLOAT,
    battery_percentage FLOAT
);

-- Gateway Table
CREATE TABLE gateway (
    gateway_id VARCHAR(50),
    device_id VARCHAR(50),
    longitude FLOAT,
    latitude FLOAT,
    altitude FLOAT,
    avg_rssi FLOAT,
    avg_snr FLOAT,
    max_rssi FLOAT,
    min_rssi FLOAT,
    PRIMARY KEY (gateway_id, device_id),
    FOREIGN KEY (device_id) REFERENCES device(device_id)
);

-- Weather Table
CREATE TABLE weather (
    weather_id INT PRIMARY KEY IDENTITY(1,1),
    luminosity FLOAT,
    temperature FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    device_id VARCHAR(50),
    received_at DATETIME2,
    FOREIGN KEY (device_id) REFERENCES device(device_id)
);

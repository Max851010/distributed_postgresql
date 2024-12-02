CREATE TABLE vehicle_az_ca (
    vehicle_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    state TEXT NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    speed DECIMAL(5, 2) NOT NULL,
    direction DECIMAL(5, 2),
    battery_level DECIMAL(5, 2),
    sensor_status TEXT CHECK (sensor_status IN ('Healthy', 'Faulty')),
    proximity_alert BOOLEAN DEFAULT FALSE
);


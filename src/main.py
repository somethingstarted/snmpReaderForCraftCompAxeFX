import os
import yaml
import sqlite3
import time
from pysnmp.hlapi import *
from datetime import datetime

# Function to read settings from YAML file
def read_settings(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# Function to query SNMP sensor
def query_sensor(ip, oid):
    errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(SnmpEngine(),
               CommunityData('public', mpModel=0),
               UdpTransportTarget((ip, 161)),
               ContextData(),
               ObjectType(ObjectIdentity(oid)))
    )
    
    if errorIndication or errorStatus:
        return None
    
    for varBind in varBinds:
        return int(varBind[1])

# Function to convert temperature from Celsius to Fahrenheit
def celsius_to_fahrenheit(celsius):
    return (celsius * 9/5) + 32

# Function to log sensor data to SQLite
def log_data_to_sqlite(db_path, data):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS sensor_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        sensor_ip TEXT,
                        temperature_c REAL,
                        temperature_f REAL,
                        humidity REAL)''')
    
    cursor.execute('INSERT INTO sensor_data (timestamp, sensor_ip, temperature_c, temperature_f, humidity) VALUES (?, ?, ?, ?, ?)', data)
    conn.commit()
    conn.close()

# Main function
def main():
    # Get the current folder path
    src_folder_path = os.path.dirname(os.path.abspath(__file__))
    root_folder_path = os.path.abspath(os.path.join(src_folder_path, '..'))
    settings_path = os.path.join(root_folder_path, 'settings.yaml')
    output_folder_path = os.path.join(root_folder_path, 'output')
    os.makedirs(output_folder_path, exist_ok=True)
    
    # Read settings
    settings = read_settings(settings_path)
    sensors = settings['knownSensors']
    humidity_enabled = settings.get('humidity', False)
    
    # Loop through sensors and gather data
    for sensor in sensors:
        ip = sensor['ip']
        temp_oid = '1.3.6.1.2.1.99.1.1.1.4.0'
        humidity_oid = '1.3.6.1.2.1.99.1.1.2.4.0' if humidity_enabled else None
        
        temperature = query_sensor(ip, temp_oid)
        humidity = query_sensor(ip, humidity_oid) if humidity_enabled else None
        
        if temperature is not None:
            temperature_c = temperature / 100.0
            temperature_f = celsius_to_fahrenheit(temperature_c)
        else:
            temperature_c = None
            temperature_f = None
        
        if humidity is not None:
            humidity = humidity / 100.0
        
        data = (
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            ip,
            temperature_c,
            temperature_f,
            humidity
        )
        
        # Log data to SQLite
        db_path = os.path.join(output_folder_path, 'sensor_data.db')
        log_data_to_sqlite(db_path, data)
        print(f'Logged data for {ip}: {data}')

if __name__ == '__main__':
    main()

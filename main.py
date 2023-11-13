import time
import yaml
import sys
import os
import random
from paho.mqtt import client as mqtt_client

from devices import PylogixDevice

from loguru import logger
logger.remove()
logger.add(sys.stderr, level='INFO')


def read_config():
    devices = []
    config = read_config_file()

    for device in config.get('devices'):
        name = device.get('name', None)
        ip = device.get('ip', None)
        frequency = device.get('frequency', 1)

        driver = device.get('driver', None)

        if driver == 'pylogix':
            slot = device.get('processor_slot', 0)
            device_entry = PylogixDevice(name, ip, frequency, slot)

        elif driver == 'modbus':
            raise NotImplementedError
            # device_entry = ModbusDevice(name, ip, frequency)

        #logger.info(f'Device tags are {device["tags"]}')
        for tag in device['tags']:
            
            device_entry.add_data_point(tag)

        devices.append(device_entry)
    return devices

def read_config_file(config_key=None):
    if len(sys.argv) == 2:
        config_path = f'{sys.argv[1]}'
    else:
        config_path = f'/etc/prodmon/{config_key}.config'

    logger.info(f'Getting config from {config_path}')

    if not os.path.exists(config_path):
        logger.exception(f'Config file not found! {config_path}')
        raise ValueError(f'Config file not found! {config_path}')

    with open(config_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    return config

def handle_update(topic, payload):

    result = client.publish(topic, payload, 2)

    status = result[0]
    #logger.info(f"tried sending {topic} : {payload}")

    if status == 0:
        logger.info(f"Sent {topic} : {payload}")
    else:
        logger.warning(f"MQTT send failed {topic} {payload}")

# used by mqtt broker on_disconnect()
FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60
FLAG_EXIT = False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info('Connected to MQTT Broker!')
    else:
        logger.info(f'Failed to connect, return code {rc}\n')

def on_disconnect(client, userdata, rc):
    logger.warning(f'Disconnected with result code: {rc}')
    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        logger.warning(f'Reconnecting in {reconnect_delay} seconds...')
        time.sleep(reconnect_delay)

        try:
            client.reconnect()
            logger.warning('Reconnected successfully!')
            return
        except Exception as err:
            logger.error(f'{err}. Reconnect failed. Retrying...')

        reconnect_delay *= RECONNECT_RATE
        reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logger.error('Reconnect failed after {reconnect_count} attempts. Exiting...')
    global FLAG_EXIT
    FLAG_EXIT = True

@logger.catch
def main():
    devices = read_config()

    broker = 'pmdsdata12'
    port = 1883
    client_id = f'mqtt-pub'
    # username = 'emqx'
    # password = 'public'

    # Set Connecting Client ID
    global client
    client = mqtt_client.Client(client_id)
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    # client.loop_start()
    client.connect(broker, port)

    while not FLAG_EXIT:
        for device in devices:
            device.client = client
            device.poll_tags()
            client.loop()

    client.loop_stop()

if __name__ == "__main__":

    main()



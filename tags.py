from abc import ABC, abstractmethod
import time
import json

from loguru import logger

class Tag(ABC):

    def __init__(self, parent, address, frequency):
        self.parent = parent
        self.address = address
        self.type = None
        self.frequency = frequency
        self.next_read = time.time()
        self.last_value = None

    @abstractmethod
    def poll(self):
        pass

    @abstractmethod
    def format_output(self):
        pass


class PingTag(Tag):
    def __init__(self, parent, name, address, frequency):
        super().__init__(parent, address, frequency)
        self.type = 'ping'
        self.name = name

    def poll(self):
        timestamp = int(time.time())
        if self.next_read < timestamp:
            # increment now so it doesn't get missed
            self.next_read = timestamp + self.frequency

            value, error_flag = self.parent.read(self.address)
            if error_flag:
                return
            topic, payload = self.format_output(timestamp, value[0].TagName)
            logger.debug(f'Create PING for {self.name} ({timestamp})')

            result = self.parent.client.publish(topic, payload, 2)
            status = result[0]

            if status == 0:
                logger.info(f"Sent {topic} : {payload}")
            else:
                logger.warning(f"MQTT send failed {topic} {payload}")


    def format_output(self, timestamp, value):
        topic = f'ping/{self.name}'
        data = json.dumps({"timestamp":timestamp,
                           "name":self.parent.name})
        return topic, data


class CounterTag(Tag):

    def __init__(self, parent, address, scale, frequency, machine, part_number_text_tag, part_number_index_tag, part_dict):
        super().__init__(parent, address, frequency)
        self.type = 'counter'
        self.db_machine_data = machine
        self.scale = scale
        if part_number_text_tag:
            self.part_number_tag = part_number_text_tag
        elif part_number_index_tag:
            self.part_number_tag = part_number_index_tag
            self.part_dict = part_dict

    def poll(self):
        timestamp = time.time()
        if self.next_read < timestamp:
            # increment now so it doesn't get missed
            self.next_read = timestamp + self.frequency

            tags, error_flag = self.parent.read([self.address, self.part_number_tag])
            if error_flag:
                return
            count = tags[0].Value
            count *= self.scale

            part = tags[1].Value
            if hasattr(self, 'part_dict'):
                part = self.part_dict.get(part)
            

            # last_value is 0 or Null
            if not self.last_value:
                if self.last_value == 0:
                    logger.info(f'Counter Rolled over: Successfully read {self.parent.name}:{self.address} ({part}:{count})')
                else:
                    logger.info(f'First pass through: Successfully read {self.parent.name}:{self.address} ({part}:{count})')
                self.last_value = count
                return

            # no change
            if not count > self.last_value:
                return

            # create entry for new values
            for part_count in range(self.last_value + 1, count + 1):
                topic, payload = self.format_output(part_count, part, int(timestamp))
                logger.debug(f'Create enrty for {self.db_machine_data} ({part}:{part_count})')
                result = self.parent.client.publish(topic, payload, 2)
                status = result[0]

                if status == 0:
                    logger.info(f"Sent {topic} : {payload}")
                else:
                    logger.warning(f"MQTT send failed ({status}) {topic} {payload}")


            self.last_value = count

    def format_output(self, count, part, timestamp):
        # create entry for new value
        machine = self.db_machine_data
        topic = f'counter/{machine}'
        payload = {
            "asset": machine,
            "part": part,
            "timestamp": timestamp,
            "perpetualcount": count,
            "count": 1,
        }
        return topic, json.dumps(payload)





class DataTag(Tag):

    def __init__(self, parent, name, address, frequency):
        super().__init__(parent, address, frequency)
        self.type = 'data'
        self.name = name
        
        
    def poll(self):
        timestamp = time.time()
        if self.next_read < timestamp:
            # increment now so it doesn't get missed
            self.next_read = timestamp + self.frequency

            value, error_flag = self.parent.read(self.address)
            if error_flag:
                return
            
            data_point = value[0].Value
            

            if self.last_value == None:
                logger.info(f'First pass through: Successfully read {self.parent.name}:{self.address} ({value[0].TagName}:{data_point})')
                
            if data_point != self.last_value:
                logger.info(f'Posting Data: Successfully read {self.parent.name}:{self.address} ({value[0].TagName}:{data_point})')

                topic, payload = self.format_output(self.parent.name, value[0].TagName, self.name, data_point)
                logger.debug(f'Create enrty for ({value[0].TagName}:{data_point})')

                result = self.parent.client.publish(topic, payload, 2)
                status = result[0]

                if status == 0:
                    logger.info(f"Sent {topic} : {payload}")
                else:
                    logger.warning(f"MQTT send failed ({status}) {topic} {payload}")

            self.last_value = data_point
             
#data/parent.name/tagname/data_point

    def format_output(self, name, tag, data_name, data_point):
        # create entry for new value
        
        topic = f'data/{name}/{data_name}/'
        data = json.dumps({"data":data_point, "tag": tag})
        return topic, data


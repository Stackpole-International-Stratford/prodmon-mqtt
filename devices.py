from abc import ABC, abstractmethod
import time
import collections

from pylogix import PLC
from loguru import logger
from tags import PingTag, CounterTag, DataTag


class Device(ABC):

    def __init__(self, name, ip, frequency):
        self.name = name
        self.ip = ip
        self.frequency = frequency
        self.tag_list = []

    def add_data_point(self, tag):
        self.tag_list.append(tag)

    def poll_tags(self):
        for tag in self.tag_list:
            tag.poll()

    # @abstractmethod
    # def process_counter_tag(self, tag):
    #     pass

    @abstractmethod
    def read(self, tag):
        pass

     


class PylogixDevice(Device):

    def __init__(self, name, ip, frequency, slot):
        super().__init__(name, ip, frequency)
        self.driver = "pylogix"
        self.processor_slot = slot
        self.comm = PLC(ip_address=self.ip, slot=self.processor_slot)

    def add_data_point(self, tag):
        tag_type = tag.get('type', None)
        frequency = tag.get('frequency', 0)
        frequency = max(self.frequency, frequency)
        tag_name = tag.get('tag', None)
        parent = self

        if tag_type == 'counter':
            scale = tag.get('scale', 1)
            machine = tag.get('machine', None)

            part_number_text_tag = tag.get('part_number_text', None)
            part_number_index_tag = tag.get('part_number_index', None)
            part_dict = tag.get('part_dict', None)

            new_tag_object = CounterTag(parent, tag_name, scale, frequency, machine, part_number_text_tag, part_number_index_tag, part_dict)



        elif tag_type == 'ping':
            name = tag.get('name', None)
            new_tag_object = PingTag(parent, name, tag_name, frequency)

        elif tag_type == 'data':
            
            name = tag.get('name', None)
            new_tag_object = DataTag(parent, name, tag_name, frequency)

        else:
            raise NotImplementedError

        super().add_data_point(new_tag_object)

    def read(self, tags):
        error_flag = False
        ret = self.comm.Read(tags)
        if not isinstance(ret, collections.Iterable):
            ret = (ret,)
        for value in ret:
            if value.Status != "Success":
                logger.warning(f'Failed to read {self.name}:{value.TagName} ({value.Status})')
                error_flag = True
            else:
                logger.debug(f'Successfully read {self.name}:{value.TagName} ({value.Value})')
        return ret, error_flag


class ModbusDevice(Device):

    def __init__(self, name, ip, frequency):
        super().__init__(name, ip, frequency)
        self.driver = "modbus"
        # self.comm = ModbusClient(host=ip, auto_open=True, auto_close=True)

    def add_data_point(self, tag):
        tag_type = tag.get('type', None)
        register = tag.get('register', None)
        scale = tag.get('scale', 1)
        frequency = tag.get('frequency', 0)
        frequency = max(self.frequency, frequency)
        db_table = tag.get('table', None)
        parent = self
        tag_name = tag.get('tag', None)

        if tag_type == 'ADAM_counter':
            raise NotImplementedError
            # machine = tag.get('machine', None)
            # part_number = tag.get('part_number', None)
            # tag_object = CounterTag(parent, register, scale, frequency, db_table, machine, part_number)

        elif tag_type == 'ping':
            name = tag.get('name', None)
            tag_object = PingTag(parent, name, register, frequency, db_table)

        elif tag_type == 'data':
           #name = tag.get('name', None)
           #tag_object = DataTag(parent, name, register, frequency, db_table)
           pass

        super().add_data_point(tag_object)

    def read(self, tag):
        error_flag = False
        count = None
        regs = self.comm.read_holding_registers(tag.address, 2)

        if regs:
            count = regs[0] + (regs[1] * 65536)
            logger.debug(f'Successfully read {self.name}:{tag.address} ({count})')
        else:
            error_flag = True
            count = None
            logger.warning(f'Failed to read {self.name}:{tag.address}')

        return count, error_flag

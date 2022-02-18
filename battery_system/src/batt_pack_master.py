import can
import ctypes
import paho.mqtt.client as mqtt
import logging
from logging.handlers import RotatingFileHandler
import time
import atexit
from datetime import datetime
import json
#import sched
import threading

# Some application parameters
CAN0_BUS_TIMEOUT_TIMESPAN = float(61.0) # [s] After this timeout timespan with no traffic on the CAN Bus some error handling and "bus loss" notification will be triggered
MQTT_SERVER_TIMEOUT_TIMESPAN = 60 # [s] After this timeout timespan with no from the MQTT server some error handling will be triggered
MQTT_SERVER_COM_PORT = 1883 # [] Port on which the MQTT server expects messages
FIXED_POINT_FRACTIONAL_BITS_MAX128 = (9) # 7 Bits für die Zahl vor dem Komma (maximal 128) und 9 Bits für die Zahl nach dem Komma (Auflösung von 0,002)
FIXED_POINT_FRACTIONAL_BITS_MAX64 = (10) # 6 Bits für die Zahl vor dem Komma (maximal 64) und 10 Bits für die Zahl nach dem Komma (Auflösung von 0,001)
FIXED_POINT_FRACTIONAL_BITS_MAX32 = (11) # 5 Bits für die Zahl vor dem Komma (maximal 32) und 11 Bits für die Zahl nach dem Komma (Auflösung von 0,0005)
BATTERY_PACK_NAME_LIST = ['pack0', 'pack1'] # ATTENTION: Needs to have an entry for each battery pack that should be connected to the battery system
CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST = ['msg0', 'msg1'] # ATTENTION: Needs to have an entry for each CAN message a battery pack sends out (currently two messages are in use)
PUBLISH_MESSAGE_STATUS_TOPIC_INTERVAL_LENGTH = (62.0) # [s] Seconds to wait until the event scheduler calls the can message and topic status publish function
EVENT_CAN_MESSAGE_STATUS_PRIORITY = 1 # Priority for the event of publishing the can message status
TOPICS_PER_BATTERY_PACK_NAME_LIST = ['topic_temp', 'topic_current', 'topic_voltage', 'topic_status', 'topic_error'] # ATTENTION: Needs to have an entry for each topic for each battery pack that is send out

class RepeatedTimer(object):
  def __init__(self, interval, function, *args, **kwargs):
    self._timer = None
    self.interval = interval
    self.function = function
    self.args = args
    self.kwargs = kwargs
    self.is_running = False
    self.next_call = time.time()
    self.start()

  def _run(self):
    self.is_running = False
    self.start()
    self.function(*self.args, **self.kwargs)

  def start(self):
    if not self.is_running:
      self.next_call += self.interval
      self._timer = threading.Timer(self.next_call - time.time(), self._run)
      self._timer.start()
      self.is_running = True

  def stop(self):
    self._timer.cancel()
    self.is_running = False

"""
x is the input fixed number which is of integer datatype
e is the number of fractional bits for example in Q1.15 e = 15
"""
def fixed_to_float(x,e):
    c = abs(x)
    sign = 1 
    if x < 0:
        # convert back from two's complement
        c = x - 1 
        c = ~c
        sign = -1
    f = (1.0 * c) / (2 ** e)
    f = f * sign
    return f

"""
f is the input floating point number 
e is the number of fractional bits in the Q format. 
    Example in Q1.15 format e = 15
"""
def float_to_fixed(f,e):
    a = f * (2**e)
    b = int(round(a))
    if a < 0:
        # next three lines turns b into it's 2's complement.
        b = abs(b)
        b = ~b
        b = b + 1
    return b

def join_bytes(high_byte, low_byte):
    twos_complement = ((high_byte << 8) | low_byte)
    return  ctypes.c_short(twos_complement).value

# Returns true if bit "bit_number" is set in "bitfield"
def check_bit(bitfield, bit_number):
    return (bool)((bitfield >> bit_number) & 1)


class Batt_pack_master_runner:  
    def __init__(self, logger):
        self.logger = logger
        self.mqtt_client = None
        self.can0_bus = None
        self.mqtt_connection_ok = False
        self.mqtt_disconnected = True
        self.can_connection_ok = False
        self.data = {}
        self.message_status = {}
        self.topic_status = {}
        self.prefil_data_dict() 
        self.prefil_message_status_dict()
        self.prefil_topic_status_dict()
        self.repeated_timer_can_messages = None
        
    def prefil_data_dict(self):    
        for pack_name in BATTERY_PACK_NAME_LIST:
            self.data[pack_name] = {}
            self.data[pack_name]['temp'] = {}
            self.data[pack_name]['temp']['unit'] = 'C'
            self.data[pack_name]['temp']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
            self.data[pack_name]['current'] = {}
            self.data[pack_name]['current']['unit'] = 'A'
            self.data[pack_name]['current']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
            self.data[pack_name]['voltage'] = {}
            self.data[pack_name]['voltage']['unit'] = 'V'
            self.data[pack_name]['voltage']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
            self.data[pack_name]['status'] = {}
            self.data[pack_name]['status']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
            self.prefil_data_dict_bitfield(pack_name, 'status', 16)
            self.data[pack_name]['error'] = {}
            self.data[pack_name]['error']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
            self.prefil_data_dict_bitfield(pack_name, 'error', 16)
            
    def set_repeated_timer_can_messages(self, repeated_timer):
        self.repeated_timer_can_messages = repeated_timer
    
    def prefil_message_status_dict(self):
        datetime_obj = datetime.now(tz=None)   
        for pack_name in BATTERY_PACK_NAME_LIST:
            self.message_status[pack_name] = {}
            for msg_name in CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST:              
                self.message_status[pack_name][msg_name] = {}
                self.message_status[pack_name][msg_name]['datetime_obj'] = datetime_obj
                self.message_status[pack_name][msg_name]['message_received'] = False
                if msg_name == 'msg0':
                    self.message_status[pack_name][msg_name]['topic_temp_published'] = False
                    self.message_status[pack_name][msg_name]['topic_current_published'] = False
                if msg_name == 'msg1':
                    self.message_status[pack_name][msg_name]['topic_current_published'] = False
                    self.message_status[pack_name][msg_name]['topic_voltage_published'] = False
                    self.message_status[pack_name][msg_name]['topic_status_published']  = False
                    self.message_status[pack_name][msg_name]['topic_error_published']   = False 
    
    def prefil_topic_status_dict(self):
        datetime_obj = datetime.now(tz=None)   
        for pack_name in BATTERY_PACK_NAME_LIST:
            self.topic_status[pack_name] = {}
            for topic_name in TOPICS_PER_BATTERY_PACK_NAME_LIST:              
                self.topic_status[pack_name][topic_name] = {}
                self.topic_status[pack_name][topic_name]['datetime_obj'] = datetime_obj
                self.topic_status[pack_name][topic_name]['topic_published'] = False

    def prefil_data_dict_bitfield(self, pack_name, key_string, nof_bits):
        for i in range(nof_bits):
            bit_string = "b" + str(i)
            self.data[pack_name][key_string][bit_string] = 0
            
    def set_data_dict_bitfield(self, pack_name, key_string, nof_bits, bitfield):
        for i in range(nof_bits):
            bit_string = "b" + str(i)
            if check_bit(bitfield, i):
                self.data[pack_name][key_string][bit_string] = 1
            else:
                self.data[pack_name][key_string][bit_string] = 0
                 
        
    # The callback when this client recieves A CONNACK from the broker    
    def on_connect(self, client, userdata, flags, rc):
        #logger = logging.getLogger('root')
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        # client.subscribe("$SYS/#")   
        if rc==0:
            print("Success: Connected to MQTT server. Returned code ",rc)
            self.logger.info("Success: Connected to MQTT Server with result code "+str(rc))
            self.mqtt_connection_ok = True
            self.mqtt_disconnected = False
        else:
            print("Failed: No connection to MQTT server established. Returned code ",rc)
            self.logger.info("Failed to connected to MQTT Server with result code "+str(rc))
            self.mqtt_connection_ok = False
    
    # The callback when the MQTT broker disconnects
    def on_disconnect(self, client, userdata, rc):
        self.logger.info("MQTT server disconnected. Reason: "  +str(rc))
        print("MQTT server disconnected. Reason: "  +str(rc))
        self.mqtt_connection_ok = False
        self.mqtt_disconnected  = True
        
    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.logger.info("New message: " + msg.topic+" "+str(msg.payload))
        print(msg.topic+" "+str(msg.payload))

    def exit_run_batt_pack_master(self):
        self.logger.info("Battery pack master function terminated! Might be abnormal.")
        print("Battery pack master function terminated. Might be abnormal.")
        self.mqtt_client.loop_stop()
        self.repeated_timer_can_messages.stop()
        
    def process_can_message(self, can0_message):
        # A CAN message was received
        # Check if the message came from one of the battery packs (arbitration_id < 100)
        # Messages from battery pack 0 have IDs 0...9, from battery pack 1 have IDs 10...19, etc.
        # In this configuration each battery can only transmit 10 messages with a maximum of 8 bytes summing up to 80 bytes.
        # If this is not enough in the future this numbering needs to be adapted and the filtering mechanism (%10) changed
        if(can0_message.arbitration_id < 100):
            # The least significant number of the CAN message identifier contains messages with the same format for all battery packs
            # Check which message it is by filtering for the least significant digit
            least_significant_digit = can0_message.arbitration_id % 10
            # Find pack number of which battery pack number this data originates
            pack_number = int(can0_message.arbitration_id / 10)
            pack_string = BATTERY_PACK_NAME_LIST[pack_number]
            # Get message number of that pack
            msg_string = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[least_significant_digit]
                                
            if(least_significant_digit == 0):
                temp_sensor_0_int   = join_bytes(can0_message.data[1], can0_message.data[0])
                temp_sensor_0       = fixed_to_float(temp_sensor_0_int, FIXED_POINT_FRACTIONAL_BITS_MAX128)
                temp_sensor_1_int   = join_bytes(can0_message.data[3], can0_message.data[2])
                temp_sensor_1       = fixed_to_float(temp_sensor_1_int, FIXED_POINT_FRACTIONAL_BITS_MAX128)
                temp_sensor_2_int   = join_bytes(can0_message.data[5], can0_message.data[4])
                temp_sensor_2       = fixed_to_float(temp_sensor_2_int, FIXED_POINT_FRACTIONAL_BITS_MAX128)
                fan_current_int     = join_bytes(can0_message.data[7], can0_message.data[6])
                fan_current         = fixed_to_float(fan_current_int, FIXED_POINT_FRACTIONAL_BITS_MAX32)
                # Write decoded values into internal data structure
                self.data[pack_string]['temp']['temp0'] = temp_sensor_0
                self.data[pack_string]['temp']['temp1'] = temp_sensor_1
                self.data[pack_string]['temp']['temp2'] = temp_sensor_2               
                self.data[pack_string]['temp']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                self.data[pack_string]['current']['fan0'] = fan_current
                self.data[pack_string]['current']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                # Register the new data in the corresponding data structure
                self.message_status[pack_string][msg_string]['datetime_obj'] = datetime.now(tz=None)
                self.message_status[pack_string][msg_string]['message_received'] = True
                # Logging
                log_string = "Recieved message \'" + msg_string + "\' of " + pack_string + "."
                self.logger.info(log_string)                       
                print(log_string)
                                    
            elif(least_significant_digit == 1):
                heating_current_int   = join_bytes(can0_message.data[1], can0_message.data[0])
                heating_current       = fixed_to_float(heating_current_int, FIXED_POINT_FRACTIONAL_BITS_MAX32)
                voltage_batt_pack_int = join_bytes(can0_message.data[3], can0_message.data[2])
                voltage_batt_pack     = fixed_to_float(voltage_batt_pack_int, FIXED_POINT_FRACTIONAL_BITS_MAX64)
                status_bitfeld        = join_bytes(can0_message.data[5], can0_message.data[4])
                error_bitfeld         = join_bytes(can0_message.data[7], can0_message.data[6])
                # Write decoded values into internal data structure
                self.data[pack_string]['current']['heater0']    = heating_current
                self.data[pack_string]['current']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                self.data[pack_string]['voltage']['batt_pack']  = voltage_batt_pack
                self.data[pack_string]['voltage']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                self.set_data_dict_bitfield(pack_string, 'status', 16, status_bitfeld)
                self.data[pack_string]['status']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                self.set_data_dict_bitfield(pack_string, 'error', 16, error_bitfeld)
                self.data[pack_string]['error']['last_update'] = datetime.now(tz=None).strftime("%d-%b-%Y (%H:%M:%S.%f)")
                # Register the new data in the corresponding data structure
                self.message_status[pack_string][msg_string]['datetime_obj'] = datetime.now(tz=None)
                self.message_status[pack_string][msg_string]['message_received'] = True
                # Logging
                log_string = "Recieved message \'" + msg_string + "\' of \'" + pack_string + "\'."
                self.logger.info(log_string)                       
                print(log_string)
                
    def mqtt_preparation_and_data_publish(self):
        # This function checks if there is enough new data to publish a new MQTT topic. This is topic dependend: e.g. the topic for the three
        # temperatures can be send whenever a new "msg0" paket was recieved, because this packet contains all temperature values for that topic.
        # The "Current" topic on the other hand needs to have "msg0" AND "msg1" with new data, because the can current is part of msg0 and the
        # heater current is part of msg1.
        datetime_obj_now = datetime.now(tz=None)   
        ################### TOPIC 0 - Temperatures - batt_sys/packs/+/temp ##################################
        # Loop over all battery packs
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Topic 0 only needs msg0 to be received
            msg_name = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[0]
            message_received = self.message_status[pack_name][msg_name]['message_received']
            # If we have a new message 0 for this pack we can publish the topic
            if(message_received is True and self.message_status[pack_name][msg_name]['topic_temp_published'] is False):
                payload_string = json.dumps(self.data[pack_name]['temp'])
                topic_string = "batt_sys/packs/" + str(pack_index) + "/temp"
                # Publish Topic
                self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Set status flags for published data
                self.message_status[pack_name][msg_name]['topic_temp_published'] = True
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[0]]['datetime_obj'] = datetime_obj_now
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[0]]['topic_published'] = True
                # Logging
                log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
                print(log_string)
                self.logger.info(log_string)
                
                
        ################### TOPIC 1 - Currents - batt_sys/packs/+/current ##################################
        # Loop over all battery packs
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Topic 1 needs msg0 and msg1 to be received
            msg_name_0 = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[0]
            msg_name_1 = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[1]
            message_0_received = self.message_status[pack_name][msg_name_0]['message_received']
            message_1_received = self.message_status[pack_name][msg_name_1]['message_received']
            if(message_0_received is True and message_1_received is True and self.message_status[pack_name][msg_name_0]['topic_current_published'] is False and self.message_status[pack_name][msg_name_1]['topic_current_published'] is False):
                payload_string = json.dumps(self.data[pack_name]['current'])
                topic_string = "batt_sys/packs/" + str(pack_index) + "/current"
                # Publish Topic
                self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Set status flag for published data
                self.message_status[pack_name][msg_name_0]['topic_current_published'] = True
                self.message_status[pack_name][msg_name_1]['topic_current_published'] = True
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[1]]['datetime_obj'] = datetime_obj_now
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[1]]['topic_published'] = True
                # Logging
                log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
                print(log_string)
                self.logger.info(log_string)
                
        ################### TOPIC 2 - Voltage - batt_sys/packs/+/voltage ##################################
        # Loop over all battery packs
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Topic 2 only needs msg1 to be received
            msg_name = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[1]
            message_received = self.message_status[pack_name][msg_name]['message_received']
            if(message_received is True and self.message_status[pack_name][msg_name]['topic_voltage_published'] is False):
                payload_string = json.dumps(self.data[pack_name]['voltage'])
                topic_string = "batt_sys/packs/" + str(pack_index) + "/voltage"
                # Publish Topic
                self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Set status flag for published data
                self.message_status[pack_name][msg_name]['topic_voltage_published'] = True
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[2]]['datetime_obj'] = datetime_obj_now
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[2]]['topic_published'] = True
                # Logging
                log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
                print(log_string)
                self.logger.info(log_string)
                
        ################### TOPIC 3 - Status - batt_sys/packs/+/status ##################################
        # Loop over all battery packs
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Topic 3 only needs msg1 to be received
            msg_name = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[1]
            message_received = self.message_status[pack_name][msg_name]['message_received']
            if(message_received is True and self.message_status[pack_name][msg_name]['topic_status_published'] is False):
                payload_string = json.dumps(self.data[pack_name]['status'])
                topic_string = "batt_sys/packs/" + str(pack_index) + "/status"
                # Publish Topic
                self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Set status flag for published data
                self.message_status[pack_name][msg_name]['topic_status_published'] = True
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[3]]['datetime_obj'] = datetime_obj_now
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[3]]['topic_published'] = True
                # Logging
                log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
                print(log_string)
                self.logger.info(log_string)
                
        ################### TOPIC 4 - Error - batt_sys/packs/+/error ##################################
        # Loop over all battery packs
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Topic 3 only needs msg1 to be received
            msg_name = CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST[1]
            message_received = self.message_status[pack_name][msg_name]['message_received']
            if(message_received is True and self.message_status[pack_name][msg_name]['topic_error_published'] is False):
                payload_string = json.dumps(self.data[pack_name]['error'])
                topic_string = "batt_sys/packs/" + str(pack_index) + "/error"
                # Publish Topic
                self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
                # Set status flag for published data
                self.message_status[pack_name][msg_name]['topic_error_published'] = True
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[4]]['datetime_obj'] = datetime_obj_now
                self.topic_status[pack_name][TOPICS_PER_BATTERY_PACK_NAME_LIST[4]]['topic_published'] = True
                # Logging
                log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
                print(log_string)
                self.logger.info(log_string)
        
    def message_supervisor(self):
        # This function checks the communication for problems. It checks if all battery packs send all messages (e.g. by checking the timestamps)
        # and it publishes the system/communication status in corresponding MQTT topics. It also checks wether all topics were published and 
        # resets the corresponding flags to start a new receive and publish cycle.
        
        # Check if topics have been published and reset recieve and publish cycle for the corresponding messages and topics
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            for msg_name in CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST:
                if msg_name == 'msg0':
                    # Check if all topics with data from msg0 have been published
                    if (self.message_status[pack_name][msg_name]['topic_temp_published'] is True and self.message_status[pack_name][msg_name]['topic_current_published'] is True):
                        self.message_status[pack_name][msg_name]['message_received']        = False
                        self.message_status[pack_name][msg_name]['topic_temp_published']    = False
                        self.message_status[pack_name][msg_name]['topic_current_published'] = False
                        self.topic_status[pack_name]['topic_temp']['topic_published']       = False
                        self.topic_status[pack_name]['topic_current']['topic_published']    = False
                        # Logging
                        log_string = "\'Msg0\' of \'" + pack_name + "\' fully published. Waiting for new msg0 of this pack before publish again."
                        print(log_string)
                        self.logger.info(log_string)
                if msg_name == 'msg1':
                    # Check if all topics with data from msg1 have been published
                    if (        self.message_status[pack_name][msg_name]['topic_current_published'] is True and self.message_status[pack_name][msg_name]['topic_voltage_published'] is True
                            and self.message_status[pack_name][msg_name]['topic_status_published']  is True and self.message_status[pack_name][msg_name]['topic_error_published']   is True):
                        # Reset and start a new recieve and publish cycle
                        self.message_status[pack_name][msg_name]['message_received']        = False
                        self.message_status[pack_name][msg_name]['topic_current_published'] = False
                        self.message_status[pack_name][msg_name]['topic_voltage_published'] = False
                        self.message_status[pack_name][msg_name]['topic_status_published']  = False
                        self.message_status[pack_name][msg_name]['topic_error_published']   = False
                        self.topic_status[pack_name]['topic_current']['topic_published']    = False
                        self.topic_status[pack_name]['topic_voltage']['topic_published']    = False
                        self.topic_status[pack_name]['topic_status']['topic_published']     = False
                        self.topic_status[pack_name]['topic_error']['topic_published']      = False
                        # Logging
                        log_string = "\'Msg1\' of \'" + pack_name + "\' fully published. Waiting for new msg1 of this pack before publish again."
                        print(log_string)
                        self.logger.info(log_string)
                    
    def batt_sys_status_publish(self):
        # Check if one of the messages was not received for a certain timeout time and update corresponding flag
        datetime_obj_now = datetime.now(tz=None)   
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Construct topic name
            topic_string = "batt_sys/packs/" + str(pack_index) + "/can_messages_missing"
            payload_string = "{\"last_update\": \"" + datetime_obj_now.strftime("%d-%b-%Y (%H:%M:%S.%f)") + "\", "
            for msg_name in CAN_MESSAGE_PER_BATTERY_PACK_NAME_LIST:              
                datetime_obj_stored = self.message_status[pack_name][msg_name]['datetime_obj']
                time_diff_seconds = (datetime_obj_now - datetime_obj_stored).total_seconds()
                missing_flag = 0
                # If the timestamp difference is bigger than the can bus timeout timespan...
                if time_diff_seconds > CAN0_BUS_TIMEOUT_TIMESPAN:
                    # Set the timeout flag
                    missing_flag = 1
                # Write corresponding payload entry
                payload_entry_string = "\"" + msg_name + "\": " + str(missing_flag) + ", "
                # Add entry to payload
                payload_string = payload_string + payload_entry_string
            # Remove comma and blank from last entry
            payload_string = payload_string[:-2]
            # Add the final closing bracket
            payload_string = payload_string + "}"
            # Publish the topic for this pack
            self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
            # Logging
            log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
            print(log_string)
            self.logger.info(log_string)
        # Check if one of the topics was not published for a certain timeout time and publish corresponding batt_sys/topic_status topic
        for pack_index, pack_name in enumerate(BATTERY_PACK_NAME_LIST):
            # Construct topic name
            topic_string = "batt_sys/packs/" + str(pack_index) + "/topics_missing"
            payload_string = "{\"last_update\": \"" + datetime_obj_now.strftime("%d-%b-%Y (%H:%M:%S.%f)") + "\", "
            for topic_name in TOPICS_PER_BATTERY_PACK_NAME_LIST:              
                datetime_obj_stored = self.topic_status[pack_name][topic_name]['datetime_obj']
                time_diff_seconds = (datetime_obj_now - datetime_obj_stored).total_seconds()
                missing_flag = 0
                # If the timestamp difference is bigger than the can bus timeout timespan...
                if time_diff_seconds > CAN0_BUS_TIMEOUT_TIMESPAN:
                    # Set the timeout flag
                    missing_flag = 1
                # Write corresponding payload entry
                payload_entry_string = "\"" + topic_name + "\": " + str(missing_flag) + ", "
                # Add entry to payload
                payload_string = payload_string + payload_entry_string
            # Remove comma and blank from last entry
            payload_string = payload_string[:-2]
            # Add the final closing bracket
            payload_string = payload_string + "}"
            # Publish the topic for this pack
            self.mqtt_client.publish(topic_string, payload=payload_string, qos=0, retain=False)
            # Logging
            log_string = "Published topic: \'" + topic_string + "\' with payload: \'" + payload_string + "\'"
            print(log_string)
            self.logger.info(log_string)
    
    def run_batt_pack_master(self):
        # Try to (re)connect CAN Bus
        while not self.can_connection_ok:
            try:
                # Connect to already established can0 interface
                self.can0_bus = can.interface.Bus(channel='can0', bustype='socketcan_native')
                self.can_connection_ok = True
                self.logger.info("Successfully connected to CAN0 interface.")
                print("Successfully connected to CAN0 interface.")
                
                # Configuration of the MQTT Client object
                self.mqtt_client = mqtt.Client()
                self.mqtt_client.username_pw_set(username="mqtt_user", password="Dy33aN")
                self.mqtt_client.on_connect = self.on_connect
                #client.on_message = self.on_message #Currently not needed because this script does not subscribe to any messages
                self.mqtt_client.on_disconnect = self.on_disconnect
                
                try:
                    self.mqtt_client.connect('localhost', port=MQTT_SERVER_COM_PORT, keepalive=MQTT_SERVER_TIMEOUT_TIMESPAN, bind_address="")
                    self.logger.info("Try to connect to MQTT Server: localhost on port " + str(MQTT_SERVER_COM_PORT) + " with timeout of " + str(MQTT_SERVER_TIMEOUT_TIMESPAN) + "s")
                    print("Try to connect to MQTT Server: localhost on port " + str(MQTT_SERVER_COM_PORT) + " with timeout of " + str(MQTT_SERVER_TIMEOUT_TIMESPAN) + "s")
                    self.mqtt_client.loop_start()
                    while not self.mqtt_connection_ok: #wait in loop until connected by on_connect callback function
                        print("Waiting for MQTT Server Connection...")
                        self.logger.info("Waiting for MQTT Server Connection...")
                        time.sleep(1)
                except:
                    self.logger.error('Could not establish connection to MQTT Server. Critical error.')
                    print("Could not establish connection to MQTT Server. Critical error.")
                    self.mqtt_client.loop_stop()
                    self.mqtt_connection_ok = False
                    pass

                # Continuously receive CAN messages and write them into MQTT topics if a connection to the MQTT server could be established
                while self.mqtt_connection_ok is True:
                    try: 
                        can0_message = self.can0_bus.recv(timeout=CAN0_BUS_TIMEOUT_TIMESPAN)
                    
                        if(can0_message is None):
                            # CAN Bus connection timed out - no battery is sending anything:
                            log_string = "CAN Bus connection timed out - Bus is completely dead. Listening again for timeout timespan " + str(CAN0_BUS_TIMEOUT_TIMESPAN) + "s."
                            print(log_string)
                            self.logger.error(log_string)
                        else:
                            # Process the CAN message
                            self.process_can_message(can0_message)
                            # Publish data if new data is completely received
                            self.mqtt_preparation_and_data_publish()
                            # Call message supervisor/handler
                            self.message_supervisor()
                            # Attention: Sending the batt_sys status MQTT topic is done asynchron with scheduled events. Not part of sequential data flow
                            
                    except can.CanError:
                        # Fill the CAN Bus error topic with the corresponding error code
                        self.logger.error('Error while reading from CAN0.')
                        print('Error while reading from CAN0.')
                        pass
            except:
                # if connection to can0 fails TODO
                self.logger.error('Could not establish connection to CAN0. Critical error. Retry in 10 seconds.')
                print("Could not establish connection to CAN0. Critical error. Retry in 10 seconds.")
                self.can_connection_ok = False
                time.sleep(10) # Wait for the next connection attempt
                pass
    

if __name__ == "__main__":
    ######## Logger Config ###############
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    my_handler = RotatingFileHandler('batt_pack_master.log', mode='a', maxBytes=5*1024*1024, 
                                    backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.INFO)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)
    
    ####### Create runner object ###############################
    batt_pack_runner = Batt_pack_master_runner(app_log)
    ####### Register timer for periodic function calls #########
    rt_can_messages = RepeatedTimer(PUBLISH_MESSAGE_STATUS_TOPIC_INTERVAL_LENGTH, batt_pack_runner.batt_sys_status_publish)
    batt_pack_runner.set_repeated_timer_can_messages(rt_can_messages)
    ######### Start the application ############################
    try:
        batt_pack_runner.run_batt_pack_master()
    finally:
        batt_pack_runner.exit_run_batt_pack_master()

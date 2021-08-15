import signal
import uuid
from typing import Dict, Optional, List, Union, Any, Tuple

from kafka.producer.future import FutureRecordMetadata
from pyftsm.ftsm import FTSM, FTSMTransitions
import kafka
from kafka import KafkaConsumer, KafkaProducer
from jsonschema import validate, ValidationError
import json
from bson import json_util
import numpy as np
import rospy
from abc import abstractmethod
from component_monitoring_enumerations import MessageType, Response


class ComponentSMBase(FTSM):
    """
    A class used to implement FTSM in the robot component

    ...

    Attributes
    ----------
    name : str
        Name of the comonent

    component_id : str,
        Unique id of the component

    dependencies : list
        List of the components on which current component is dependant

    monitoring_control_topic : str
        Name of the topic used to switch off and on the monitors

    monitoring_pipeline_server : str
        Address and port of the server used to communicate with the component monitoring 
        e.g. default address of the Kafka server is 'localhost:9092'

    monitoring_feedback_topics : list[str]
        Name of the topics to receive feedback from the monitors

    monitors_ids : list[str]
        List of the unique ids of the monitors that are monitoring the current component

    request_message_format : dict
        Format of the message used to switch on and of monitors

    request_message_schema : dict
        Schema of the message used to switch on and off monitors

    monitoring_message_schemas : list[dict]
        Schemas of the messages received from the monitors as feedback
                
    monitoring_timeout : int
        Time in seconds after which the feedback from the monitors is considered as not received

    max_recovery_attempts : int
        Maximum number of attempts to recover
    """

    def __init__(self,
                 name,
                 component_id,
                 monitor_manager_id,
                 dependencies,
                 monitoring_control_topic,
                 monitoring_pipeline_server,
                 monitors_ids,
                 request_message_format,
                 response_message_schema,
                 request_message_schema,
                 broadcast_message_schema,

                 monitoring_message_schemas,
                 monitoring_timeout=5,
                 max_recovery_attempts=1,
                 ):

        super(ComponentSMBase, self).__init__(name,
                                              dependencies,
                                              max_recovery_attempts)
        self._to_be_monitored = False
        self._id = component_id
        self._monitor_manager_id = monitor_manager_id
        self._monitors_ids = dict()
        for monitor_id in monitors_ids:
            self._monitors_ids[monitor_id] = None
        self._monitoring_message_schemas = monitoring_message_schemas
        self._request_message_schema = request_message_schema
        self._request_message_format = request_message_format
        self._broadcast_message_schema = broadcast_message_schema
        self._response_message_schema = response_message_schema
        self._monitoring_control_topic = monitoring_control_topic
        self._monitoring_pipeline_server = monitoring_pipeline_server
        self._monitoring_feedback_topics = []
        self._monitoring_timeout = monitoring_timeout

        # Kafka monitor control producer placeholder
        self._monitor_control_producer = None

        # Kafka monitor control listener placeholder
        self._monitor_control_listener = None

        # Kafka monitor feedback listener placeholder
        self._monitor_feedback_listener = None

        # flag if kafka communication is working
        self._is_kafka_available = False

        # connecting to kafka
        self.__connect_to_control_pipeline()

    @abstractmethod
    def handle_monitoring_feedback(self):
        '''
        Function for handling messages from the monitors responsible for monitoring the current component.
        '''
        pass

    def __connect_to_control_pipeline(self):
        retry_counter = 0

        while not self._is_kafka_available and retry_counter < 3:
            retry_counter += 1
            self._is_kafka_available = self.__init_control_pipeline()
            if not self._is_kafka_available:
                rospy.logwarn('[{}][{}] No kafka server detected. Retrying ...'.
                              format(self.name, self._id))
                rospy.sleep(5)

        if self._is_kafka_available:
            rospy.logwarn('[{}][{}] Kafka server detected. Component will try to start with monitoring.'.
                          format(self.name, self._id))
        else:
            rospy.logwarn('[{}][{}] No kafka server detected. Component will start without monitoring.'.
                          format(self.name, self._id))

    def __init_listener(self, topics):
        listener = \
            KafkaConsumer(
                *topics,
                bootstrap_servers=self._monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=self._monitoring_timeout * 1000
            )
        return listener

    def __init_producer(self):
        producer = \
            KafkaProducer(
                bootstrap_servers=self._monitoring_pipeline_server
            )
        return producer

    def __init_monitor_feedback_listener(self, topics):
        self._monitoring_feedback_topics = topics
        self._monitor_feedback_listener = self.__init_listener(topics)

    def __init_control_pipeline(self):
        try:
            # Kafka monitor producer listener
            self._monitor_control_producer = self.__init_producer()

            # Kafka monitor control listener
            self._monitor_control_listener = \
                self.__init_listener([self._monitoring_control_topic])

            return True

        except kafka.errors.NoBrokersAvailable as err:
            # Kafka monitor control producer placeholder
            self._monitor_control_producer = None

            # Kafka monitor control listener placeholder
            self._monitor_control_listener = None
            return False

    def __create_message_header(self) -> Tuple[Dict, str]:
        message = self._request_message_format.copy()
        dialogue_id = str(uuid.uuid4())
        message['Id'] = dialogue_id
        message['From'] = self._id
        return message, dialogue_id

    def __send_start(self, params: Dict = None) -> bool:
        message, dialogue_id = self.__create_message_header()
        message['To'] = self._monitor_manager_id
        message[MessageType.START.value] = list()
        for monitor in self._monitors_ids:
            element = dict()
            element['Id'] = monitor
            if params:
                try:
                    element['Params'] = params[monitor]
                except KeyError:
                    pass
            message[MessageType.START.value].append(element)
        if self.__send_control_cmd(message):
            success, reply = self.__receive_control_response(dialogue_id, [Response.STARTED, Response.OKAY])
            if success and isinstance(reply, list):
                print(reply)
                for monitor in reply:
                    self._monitoring_feedback_topics.append(monitor['topic'])
                    self._monitors_ids[monitor['mode']] = monitor['id']
                return True
        return False

    def __send_update(self, receiver: str, params: Dict[str, Any]) -> bool:
        message, dialogue_id = self.__create_message_header()
        message['To'] = receiver
        update = dict()
        update['Params'] = params
        message[MessageType.UPDATE.value] = update
        if self.__send_control_cmd(message):
            success, reply = self.__receive_control_response(dialogue_id, [Response.OKAY], response_timeout=10)
            return success
        return False

    def __send_stop(self) -> bool:
        message, dialogue_id = self.__create_message_header()
        message['To'] = self._monitor_manager_id
        message[MessageType.START.value] = list()
        for monitor in self._monitors_ids:
            element = dict()
            element['Id'] = monitor
            message[MessageType.START.value].append(element)
        if self.__send_control_cmd(message):
            success, reply = self.__receive_control_response(dialogue_id, [Response.STOPPED])
            if success and isinstance(reply, list):
                print(reply)
                for monitor in reply:
                    self._monitoring_feedback_topics.remove(monitor['topic'])
                    self._monitors_ids[monitor['mode']] = monitor['id']
                return True
        return False

    def __toggle_storage(self, monitor_ids: Dict[str, str], on: bool) -> bool:
        ids = list(monitor_ids.copy().keys())
        if len(self._monitoring_feedback_topics) == len(monitor_ids):
            retries = 3
            while retries > 0:
                for monitor in ids:
                    monitor_id = self._monitors_ids[monitor]
                    if monitor_id is not None:
                        if self.__send_update(monitor_id, {"Store": on}):
                            ids.remove(monitor)
                    else:
                        rospy.logwarn("Monitor {} not initialized".format(monitor))
                if len(ids) <= 0:
                    return True
            return False
        rospy.logwarn('[{}][{}] Storage could not be started for {}'.format(self.name, self._id, monitor_ids))
        return False

    def __send_control_cmd(self, message: Dict) -> Optional[FutureRecordMetadata]:
        future = \
            self._monitor_control_producer.send(
                self._monitoring_control_topic,
                json.dumps(message,
                           default=json_util.default).encode('utf-8')
            )
        try:
            return future.get(timeout=60)
        except TimeoutError:
            rospy.logwarn(
                "[{}][{}] Timeout while waiting for message with ID {} to be sent!".format(self.name, self._id,
                                                                                           message['Id']))
            return None
        except Exception:
            return None

    def __receive_control_response(self, dialogue_id: str, expected_response_codes: List[Response],
                                   response_timeout=10) -> Tuple[bool, Any]:
        start_time = rospy.Time.now()
        try:
            for message in self._monitor_control_listener:
                try:
                    rospy.loginfo(message.value)
                    if message.value['To'] != self._id:
                        continue
                    # Validate the correctness of the response message
                    validate(
                        instance=message.value,
                        schema=self._response_message_schema
                    )
                    if message.value['To'] == self._id and message.value['Id'] == dialogue_id:
                        response_code = Response(message.value['Response']['Code'])
                        if response_code in expected_response_codes:
                            success = True
                        else:
                            success = False
                        try:
                            reply = message.value['Response']['Message']
                        except KeyError:
                            reply = None
                        if not success:
                            rospy.logwarn(
                                '[{}][{}] Received {}: {}'.format(self.name, self._id, response_code, reply))
                        return success, reply
                except ValidationError:
                    rospy.logwarn(
                        '[{}][{}] Invalid format of the acknowledgement from the monitor manager regarding monitors: {}.'.
                            format(self.name, self._id, self._monitors_ids))
                except KeyError:
                    # If message does not contain "To" field it could be a broadcast, hence this does not have to
                    # indicate an error and can be ignored.
                    pass
                if rospy.Time.now() - start_time > rospy.Duration(response_timeout):
                    raise TimeoutError

            rospy.logwarn('[{}][{}] No response from the monitor manager.'.
                          format(self.name, self._id))
            return False, None
        except TimeoutError:
            rospy.logwarn(
                '[{}][{}] Timeout occurred while waiting for response on Request with ID {}'.format(self.name, self._id,
                                                                                                    dialogue_id))
            return False, None

    def __switch(self, device: str, mode: str):
        monitoring = 'monitoring'
        database = 'database'
        on = 'on'
        off = 'off'

        if device not in [monitoring, database] or mode not in [on, off]:
            raise ValueError('device must be "monitoring"/"database", mode must be "on"/"off"')

        rospy.loginfo(
            '[{}][{}] Turning {} the {}.'.format(self.name, self._id, mode, device)
        )

        if mode == on:
            if device == monitoring:
                success = self.__send_start()
            else:
                success = self.__toggle_storage(monitor_ids=self._monitors_ids, on=True)
        else:
            if device == monitoring:
                success = self.__send_stop()
            else:
                success = self.__toggle_storage(monitor_ids=self._monitors_ids, on=False)

        if success:
            rospy.loginfo(
                '[{}][{}] Successfully turned {} the {}'.
                    format(self.name, self._id, mode, device)
            )

        else:
            rospy.logerr(
                '[{}][{}] Unsuccessfully turned {} the {}'.
                    format(self.name, self._id, mode, device)
            )

        return success

    def turn_off_monitoring(self):
        '''
        Function responsible for turning off the monitors responsible for monitoring the current component.

            Returns:
                bool: True - turning off the monitors ended successfully
                      False - turning off the monitors ended with failure
        '''
        return self.__switch(device='monitoring', mode='off')

    def turn_on_monitoring(self):
        '''
        Function responsible for turning on the monitors responsible for monitoring the current component.

            Returns:
                bool: True - turning on the monitors ended successfully
                      False - turning on the monitors ended with failure
        '''
        return self.__switch(device='monitoring', mode='on')

    def turn_on_storage(self):
        return self.__switch(device='database', mode='on')

    def turn_off_storage(self):
        return self.__switch(device='database', mode='off')

    def running(self):
        return FTSMTransitions.DONE

    def recovering(self):
        return FTSMTransitions.DONE_RECOVERING

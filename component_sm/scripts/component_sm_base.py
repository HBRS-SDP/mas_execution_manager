from pyftsm.ftsm import FTSM, FTSMTransitions
from kafka import KafkaConsumer, KafkaProducer
from jsonschema import validate, ValidationError
import json
from bson import json_util
import numpy as np
import rospy
from abc import abstractmethod
from component_monitoring_enumerations import MessageType, Command, ResponseCode


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

    general_message_format : dict
        Format of the message used to switch on and of monitors

    general_message_schema : dict
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
                 general_message_format,
                 general_message_schema,
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
        self._monitors_ids = monitors_ids
        self._monitoring_message_schemas = monitoring_message_schemas
        self._general_message_schema = general_message_schema
        self._general_message_format = general_message_format
        self._monitoring_control_topic = monitoring_control_topic
        self._monitoring_pipeline_server = monitoring_pipeline_server
        self._monitoring_feedback_topics = []
        self._monitoring_timeout = monitoring_timeout

        # Kafka monitor control producer
        self._monitor_control_producer = \
            KafkaProducer(
                bootstrap_servers=monitoring_pipeline_server
            )

        # Kafka monitor control listener
        self._monitor_control_listener = \
            KafkaConsumer(
                self._monitoring_control_topic,
                bootstrap_servers=monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=self._monitoring_timeout * 1000
            )

        # Kafka monitor feedback listener placeholder
        self._monitor_feedback_listener = None
        

    @abstractmethod
    def handle_monitoring_feedback(self):
        '''
        Function for handling messages from the monitors responsible for monitoring the current component.
        '''
        pass

    def __init_monitor_feedback_listener(self, topics):
        self._monitoring_feedback_topics = topics
        self._monitor_feedback_listener = \
            KafkaConsumer(
                *topics,
                bootstrap_servers=self._monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=self._monitoring_timeout * 1000
            )

    def __control_request(self, command: Command, response_timeout=5):
        '''
        Function responsible for sending a command to the monitors responsible for monitoring the current component.

            Parameters:
                command (Command): Command for monitor manager/storage manager to manage monitoring/storage
                response_timeout (int): Timeout to wait for response from the monitor manager
            
            Returns:
                bool: True - executiong of the command ended successfully
                      False - executing of the command ended with failure 
        '''
        message = self._general_message_format
        message['from'] = self._id
        message['to'] = self._monitor_manager_id
        message['message'] = MessageType.REQUEST.value
        message['body']['command'] = command.value

        if command in [Command.START_STORE, Command.STOP_STORE]:
            monitors = list()
            if not self._monitoring_feedback_topics:
                rospy.logwarn('[{}][{}] Attempted to run database component, but topics names with events were not received')
                return False
            for monitor, topic in zip(self._monitors_ids, self._monitoring_feedback_topics):
                monitors.append({"name": monitor, "topic": topic})
            message['body']['monitors'] = monitors
        else:
            message['body']['monitors'] = self._monitors_ids

        future = \
            self._monitor_control_producer.send(
                self._monitoring_control_topic,
                json.dumps(message,
                           default=json_util.default).encode('utf-8')
            )

        result = future.get(timeout=60)

        try:
            start_time = rospy.Time.now()

            for message in self._monitor_control_listener:

                # Validate the correctness of the message
                validate(
                    instance=message.value,
                    schema=self._general_message_schema
                )
                message_type = MessageType(message.value['message'])

                #if self._id != message.value['to']:
                #    continue

                if self._id == message.value['to'] and \
                    self._monitor_manager_id == message.value['from'] and \
                    message_type == MessageType.RESPONSE:
                    response_code = ResponseCode(message.value['body']['code'])
                    if response_code == ResponseCode.SUCCESS:
                        if command == Command.START:
                            for monitor in message.value['body']['monitors']:
                                topics = [0]*len(self._monitors_ids)
                                index = self._monitors_ids.index(monitor['name'])
                                topics[index] = monitor['topic']
                                self.__init_monitor_feedback_listener(topics)
                            rospy.loginfo('[{}][{}] Received kafka topics for monitors: {}. The topics are: {}.'.
                                  format(self.name, self._id, self._monitors_ids, self._monitoring_feedback_topics))
                        return True
                    else:
                        return False

                if rospy.Time.now() - start_time > rospy.Duration(response_timeout):
                    rospy.logwarn('[{}][{}] Obtaining only responses from the monitor manager with incorrect data.'.
                                  format(self.name, self._id))
                    return False

            rospy.logwarn('[{}][{}] No response from the monitor manager.'.
                          format(self.name, self._id))
            return False

        except ValidationError:
            rospy.logwarn(
                '[{}][{}] Invalid format of the acknowledgement from the monitor manager regarding monitors: {}.'.
                    format(self.name, self._id, self._monitors_ids))
            return False

    def turn_off_monitoring(self):
        '''
        Function responsible for turning off the monitors responsible for monitoring the current component.
            
            Returns:
                bool: True - turning off the monitors ended successfully
                      False - turning off the monitors ended with failure 
        '''
        if self._to_be_monitored:
            rospy.logwarn(
                '[{}][{}] Turning off the monitoring.'.
                    format(self.name, self._id)
            )

            success = self.__control_request(command=Command.SHUTDOWN)

            if success:
                self._to_be_monitored = False

                rospy.logwarn(
                    '[{}][{}] Successfully turned off the monitoring'.
                        format(self.name, self._id)
                )

            else:
                rospy.logerr(
                    '[{}][{}] Unsuccessfully turned off the monitoring'.
                        format(self.name, self._id)
                )

            return success

        self._to_be_monitored = False
        return True

    def turn_on_monitoring(self):
        '''
        Function responsible for turning on the monitors responsible for monitoring the current component.
            
            Returns:
                bool: True - turning on the monitors ended successfully
                      False - turning on the monitors ended with failure 
        '''
        if not self._to_be_monitored:
            rospy.loginfo(
                '[{}][{}] Turning on the monitoring.'.
                    format(self.name, self._id)
            )

            success = self.__control_request(command=Command.START)
            success = self.__control_request(command=Command.START_STORE)

            if success:
                self._to_be_monitored = True

                rospy.loginfo(
                    '[{}][{}] Successfully turned on the monitoring.'.
                        format(self.name, self._id)
                )

            else:
                rospy.logerr(
                    '[{}][{}] Unsuccessfully turned on the monitoring.'.
                        format(self.name, self._id)
                )

            return success

        self._to_be_monitored = True
        return True

    def running(self):
        return FTSMTransitions.DONE

    def recovering(self):
        return FTSMTransitions.DONE_RECOVERING

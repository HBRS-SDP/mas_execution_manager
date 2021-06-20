from pyftsm.ftsm import FTSM, FTSMTransitions
from kafka import KafkaConsumer, KafkaProducer
from jsonschema import validate, ValidationError
import json
from bson import json_util
import numpy as np
import rospy
from abc import abstractmethod

class ComponentSMBase(FTSM):
    """
    A class used to implement FTSM in the robot component

    ...

    Attributes
    ----------
    name : str
        name of the comonent

    component_id : str,
        unique id of the component

    dependencies : list
        list of the components on which current component is dependant

    monitoring_control_topic : str
        name of the topic used to switch off and on the monitors

    monitoring_pipeline_server : str
        address and port of the server used to communicate with the component monitoring 
        e.g. default address of the Kafka server is 'localhost:9092'

    monitoring_feedback_topics : list[str]
        name of the topics to receive feedback from the monitors

    monitors_ids : list[str]
        list of the unique ids of the monitors that are monitoring the current component

    general_message_format : dict
        format of the message used to switch on and of monitors

    general_message_schema : dict
        schema of the message used to switch on and off monitors

    monitoring_message_schemas : list[dict]
        schemas of the messages received from the monitors as feedback
                
    monitoring_timeout : int
        time in seconds after which the feedback from the monitors is considered as not received

    max_recovery_attempts : int
        maximum number of attempts to recover
    """

    def __init__(self, 
                name, 
                component_id,
                dependencies, 
                monitoring_control_topic,
                monitoring_pipeline_server,
                monitoring_feedback_topics,
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
        self._monitors_ids = monitors_ids
        self._monitoring_message_schemas = monitoring_message_schemas
        self._general_message_schema = general_message_schema
        self._general_message_format = general_message_format
        self._monitoring_control_topic = monitoring_control_topic  
        self._monitoring_pipeline_server = monitoring_pipeline_server
        self._monitoring_feedback_topics = monitoring_feedback_topics
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
                consumer_timeout_ms=self._monitoring_timeout*1000
                )

        # Kafka monitor feedback listener
        self._monitor_feedback_listener = \
            KafkaConsumer(
                *self._monitoring_feedback_topics, 
                bootstrap_servers=monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=self._monitoring_timeout*1000
                )
        
    @abstractmethod
    def handle_monitoring_feedback(self):
        pass

    def __control_monitoring(self, cmd, response_timeout = 5):
        message = self._general_message_format
        message['source_id'] = self._id
        message['target_id'] = self._monitors_ids
        message['message']['command'] = cmd
        message['message']['status'] = ''
        message['type'] = 'cmd'
        
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

                if self._id in message.value['target_id'] and \
                    message.value['type'] == 'ack' and \
                        message.value['message']['status'] == 'success':
                            return True

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
        if self._to_be_monitored:
            rospy.logwarn(
                '[{}][{}] Turning off the monitoring.'.
                format(self.name, self._id)
            )

            success = self.__control_monitoring(cmd='shutdown')

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
        if not self._to_be_monitored:
            rospy.loginfo(
                '[{}][{}] Turning on the monitoring.'.
                format(self.name, self._id)
            )

            success = self.__control_monitoring(cmd='activate')

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
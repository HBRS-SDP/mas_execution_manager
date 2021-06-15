#!/usr/bin/python
import rospy
from pyftsm.ftsm import FTSMTransitions
from component_sm import ComponentSM
from sensor_msgs.msg import PointCloud2
from kafka import KafkaConsumer, KafkaProducer
from jsonschema import validate, ValidationError
import json
from bson import json_util
import math
import numpy as np

###################################################################
#TOD:
#  - enable loading a list of input and output data topics
#  - move yaml, json and schema files to more reasonable place
#  - enable switching on and off the component monitoring
###################################################################

class StreamRGBDSM(ComponentSM):
    def __init__(self, 
                 component_id,
                 data_input_topic,
                 data_output_topic,
                 monitoring_control_topic,
                 monitoring_pipeline_server,
                 monitor_feedback_topic,
                 monitor_id,
                 general_message_format,
                 general_message_schema,
                 monitoring_message_schema,
                 data_transfer_timeout,
                 max_recovery_attempts=1):
        
        super(StreamRGBDSM, self).__init__('StreamRGBD', [], max_recovery_attempts)
        
        self._id = component_id
        self._monitor_id = monitor_id
        self._timeout = data_transfer_timeout
        self._pointcloud = None
        self._monitoring_message_schema = monitoring_message_schema
        self._general_message_schema = general_message_schema
        self._general_message_format = general_message_format
        self._to_be_monitored = False
        self._monitoring_control_topic = monitoring_control_topic
        
        # Kafka monitor control producer
        self._monitor_control_producer = \
            KafkaProducer(
                bootstrap_servers=monitoring_pipeline_server
                )

        # Kafka monitor control listener
        self._monitor_control_listener = \
            KafkaConsumer(
                monitoring_control_topic, 
                bootstrap_servers=monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=3000
                )
        
        # Kafka monitor feedback listener
        self._monitor_feedback_listener = \
            KafkaConsumer(
                monitor_feedback_topic, 
                bootstrap_servers=monitoring_pipeline_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=3000
                )

        # ROS poincloud listener
        self._pointcloud_listener = \
            rospy.Subscriber(
                data_input_topic, 
                PointCloud2,
                self._callback
                )

        # ROS pointcloud publisher
        self._pointcloud_publisher = \
            rospy.Publisher(
                data_output_topic, 
                PointCloud2, 
                queue_size=10
                )

        self._last_active_time = rospy.Time.now()

    def _callback(self, data):
        # Receiving pointcloud
        self._last_active_time = rospy.Time.now()
        self.execution_requested = True
        self._pointcloud = data

        # Publishing poincloud to component monitoring
        #rospy.loginfo('Now I publish pointcloud to component monitoring')
        self._pointcloud_publisher.publish(self._pointcloud)

    def control_monitoring(self, cmd):
        message = self._general_message_format
        message['source_id'] = self._id
        message['target_id'] = [self._monitor_id]
        message['message']['command'] = cmd
        message['message']['status'] = ''
        message['message']['thread_id'] = ''
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
            max_duration = 5

            for message in self._monitor_control_listener:
                # Validate the correctness of the message
                validate(
                    instance=message.value, 
                    schema=self._general_message_schema
                    )

                if message.value['source_id'] == self._monitor_id and \
                    self._id in message.value['target_id'] and \
                        message.value['type'] == 'ack' and \
                            message.value['message']['status'] == 'success':
                            return True
                if rospy.Time.now() - start_time > rospy.Duration(max_duration):
                    return False
            
            return False

        except ValidationError:
            return False

    def turn_off_monitoring(self):
        if self._to_be_monitored:
            success = self.control_monitoring(cmd='shutdown')

            if success:
                self._to_be_monitored = False

            rospy.logwarn(
                'Turning off the monitoring. Success: {}'.
                format(success)
            )

            return success

        self._to_be_monitored = False
        return True

    def turn_on_monitoring(self):
        if not self._to_be_monitored:
            success = self.control_monitoring(cmd='activate')

            if success:
                self._to_be_monitored = True
                
            rospy.loginfo(
                'Turning on the monitoring. Success: {}'.
                format(success)
                )

            return success

        self._to_be_monitored = True
        return True

    def running(self):
        # Receiving events from component monitoring
        time_now = rospy.Time.now()
        while time_now - rospy.Duration(self._timeout) < \
           self._last_active_time and self._pointcloud.data :
            
            self.turn_on_monitoring()

            time_now = rospy.Time.now()
             
            last_message = None
            try:
                for message in self._monitor_feedback_listener:
                    # Validate the correctness of the message
                    validate(instance=message.value, schema=self._monitoring_message_schema)

                    self.turn_on_monitoring()
                    
                    last_message = message

                    time_now = rospy.Time.now()
                    if not time_now - rospy.Duration(self._timeout) < self._last_active_time:
                        break

                    if message.value['healthStatus']['nans']:
                        rospy.logerr('Received poincloud conatins to many NaN values.')
                        return FTSMTransitions.RECOVER
                    else:
                        rospy.loginfo('Received poincloud contains acceptable number of NaN values.')
                
                if last_message is None and self._to_be_monitored:
                    rospy.logwarn('No feedback from the monitor.')

            except ValidationError:
                rospy.logwarn('Invalid format of the feedback message from the monitor.')
            
            rospy.sleep(0.1)
            
            #return FTSMTransitions.DONE

        rospy.logerr('Can not receive the poincloud from head RGBD Camera.')
        self.turn_off_monitoring()
        return FTSMTransitions.RECONFIGURE

    def recovering(self):
        rospy.loginfo('Now I am recovering the RGBD CAMERA by moving the head')
        # Code responsible for recovering the camera e.g. moving the head
        rospy.sleep(3)
        return FTSMTransitions.DONE_RECOVERING

    def configuring(self):
        rospy.loginfo('Now I am reconfiguring the head RGBD camera by resetting the message bus')
        # Code responsible for reconfiguring the camera e.g. resetting the message bus 
        rospy.sleep(5)
        return FTSMTransitions.DONE_CONFIGURING
#!/usr/bin/python
import rospy
from pyftsm.ftsm import FTSMTransitions
from component_sm import ComponentSM
from sensor_msgs.msg import PointCloud2
from kafka import KafkaConsumer
from jsonschema import validate, ValidationError
import json
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
                 data_input_topics,
                 data_output_topics,
                 monitoring_pipeline_topic,
                 monitoring_pipeline_server,
                 data_transfer_timeout,
                 monitoring_message_schema,
                 general_message_format,
                 max_recovery_attempts=1):
        
        super(StreamRGBDSM, self).__init__('StreamRGBD', [], max_recovery_attempts)
        
        self.timeout = data_transfer_timeout
        self.pointcloud = None
        self.monitoring_message_schema = monitoring_message_schema
        
        # Kafka event listener
        self.event_listener = \
        KafkaConsumer(monitoring_pipeline_topic, 
                      bootstrap_servers=monitoring_pipeline_server,
                      value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                      consumer_timeout_ms=3000)

        # ROS poincloud listener
        self.pointcloud_listener = rospy.Subscriber(data_input_topics, 
                                                    PointCloud2,
                                                    self._callback)

        # ROS pointcloud publisher
        self.pointcloud_publisher = rospy.Publisher(data_output_topics, 
                                                    PointCloud2, 
                                                    queue_size=10)

        self.last_active_time = rospy.Time.now()

    def _callback(self, data):
        # Receiving pointcloud
        self.last_active_time = rospy.Time.now()
        self.execution_requested = True
        self.pointcloud = data

        # Publishing poincloud to component monitoring
        #rospy.loginfo('Now I publish pointcloud to component monitoring')
        self.pointcloud_publisher.publish(self.pointcloud)

    def running(self):
        # Receiving events from component monitoring
        time_now = rospy.Time.now()
        while time_now - rospy.Duration(self.timeout) < \
           self.last_active_time and self.pointcloud.data :
            
            time_now = rospy.Time.now()
             
            last_message = None
            try:

                for message in self.event_listener:
                    
                    validate(instance=message.value, schema=self.monitoring_message_schema)
                    print(message.value)
                    last_message = message

                    time_now = rospy.Time.now()
                    if not time_now - rospy.Duration(self.timeout) < self.last_active_time:
                        break

                    if message.value['healthStatus']['nans']:
                        rospy.logerr('Received poincloud conatins to many NaN values.')
                        return FTSMTransitions.RECOVER
                    else:
                        rospy.loginfo('Received poincloud conatins acceptable number of NaN values.')
                
                if last_message is None:
                    rospy.logwarn('No feedback from the monitor.')

            except ValidationError:
                rospy.logwarn('Invalid format of the feedback message from the monitor.')
            
            rospy.sleep(0.1)
            
            #return FTSMTransitions.DONE

        rospy.logerr('Can not receive the poincloud from head RGBD Camera.')
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
#!/usr/bin/python
import rospy
from pyftsm.ftsm import FTSMTransitions
from component_sm import ComponentSM
from sensor_msgs.msg import PointCloud2
from kafka import KafkaConsumer
import json
import math
import numpy as np


class StreamRGBDSM(ComponentSM):
    def __init__(self, 
                 timeout=5.0, 
                 input_pointcloud_topic='/hsrb/head_rgbd_sensor/depth_registered/points', 
                 output_pointcloud_topic='/hsrb/head_rgbd_sensor/pointcloud',
                 input_event_topic='hsrb_monitoring_rgbd',
                 max_recovery_attempts=1):
        
        super(StreamRGBDSM, self).__init__('StreamRGBD', [], max_recovery_attempts)
        
        self.timeout = timeout
        self.pointcloud = None
        
        # Kafka event listener
        self.event_listener = KafkaConsumer(input_event_topic,
                                            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                            consumer_timeout_ms=100)

        # ROS poincloud listener
        self.pointcloud_listener = rospy.Subscriber(input_pointcloud_topic, 
                                                    PointCloud2,
                                                    self._callback)

        # ROS pointcloud publisher
        self.pointcloud_publisher = rospy.Publisher(output_pointcloud_topic, 
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

             
            for message in self.event_listener:
                if message.value['healthStatus']['nans']:
                    rospy.logerr('Received poincloud conatins to many NaN values.')
                    return FTSMTransitions.RECOVER
                else:
                    rospy.loginfo('Received poincloud conatins acceptable number of NaN values.')
            
            
            rospy.logwarn('No feedback from the monitor.')
            rospy.sleep(5)
            
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
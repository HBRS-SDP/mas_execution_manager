#!/usr/bin/python
import rospy
from pyftsm.ftsm import FTSMTransitions
from component_sm_base import ComponentSMBase
from sensor_msgs.msg import PointCloud2
from jsonschema import validate, ValidationError
import json
import numpy as np

class RGBDCameraSM(ComponentSMBase):
    def __init__(self, 
                 component_id,
                 data_input_topic,
                 data_output_topic,
                 monitoring_control_topic,
                 monitoring_pipeline_server,
                 monitoring_feedback_topics,
                 monitors_ids,
                 general_message_format,
                 general_message_schema,
                 monitoring_message_schema,
                 data_transfer_timeout,
                 max_recovery_attempts=1):
        super(RGBDCameraSM, self).__init__('RGBDCameraSM', 
                                           component_id=component_id,
                                           dependencies=[], 
                                           max_recovery_attempts=max_recovery_attempts,
                                           monitoring_control_topic=monitoring_control_topic,
                                           monitoring_pipeline_server=monitoring_pipeline_server,
                                           monitoring_feedback_topics=monitoring_feedback_topics,
                                           monitors_ids=monitors_ids,
                                           general_message_format=general_message_format,
                                           general_message_schema=general_message_schema,
                                           monitoring_message_schema=monitoring_message_schema,
                                           monitoring_timeout=5)
        
        self._timeout = data_transfer_timeout
        self._pointcloud = None
        self._timeout = data_transfer_timeout

        # ROS poincloud listener
        self._pointcloud_listener = \
            rospy.Subscriber(
                data_input_topic, 
                PointCloud2,
                self.__callback
                )

        # ROS pointcloud publisher
        self._pointcloud_publisher = \
            rospy.Publisher(
                data_output_topic, 
                PointCloud2, 
                queue_size=10
                )

        self._last_active_time = rospy.Time.now()

    def __callback(self, data):
        # Receiving pointcloud
        self._last_active_time = rospy.Time.now()
        #self.execution_requested = True
        self._pointcloud = data

        # Publishing poincloud to component monitoring
        #rospy.loginfo('Now I publish pointcloud to component monitoring')
        self._pointcloud_publisher.publish(self._pointcloud)

    def __handle_monitoring_feedback(self):
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
                    rospy.logerr('[{}][{}] Received poincloud contains to many NaN values.'.
                    format(self.name, self._id))
                    return FTSMTransitions.RECOVER
                else:
                    rospy.loginfo('[{}][{}] Received poincloud contains acceptable number of NaN values.'.
                    format(self.name, self._id))
            
            if last_message is None and self._to_be_monitored:
                rospy.logwarn('[{}][{}] No feedback from the monitor.'.
                format(self.name, self._id))

        except ValidationError:
            rospy.logwarn('[{}][{}] Invalid format of the feedback message from the monitor.'.
            format(self.name, self._id))

    def running(self):
        # Receiving events from component monitoring
        
        monitor_feedback_handling_result = None
        time_now = rospy.Time.now()

        while time_now - rospy.Duration(self._timeout) < \
           self._last_active_time and self._pointcloud.data :
            time_now = rospy.Time.now()
            
            self.turn_on_monitoring()

            monitor_feedback_handling_result = self.__handle_monitoring_feedback()    

        if monitor_feedback_handling_result is None:
            rospy.logerr('[{}][{}] Can not receive the poincloud from head RGBD Camera.'.
            format(self.name, self._id))
            self.turn_off_monitoring()
            return FTSMTransitions.RECONFIGURE
        
        else:
            return monitor_feedback_handling_result

    def recovering(self):
        rospy.loginfo('[{}][{}] Now I am recovering the RGBD CAMERA by moving the head'.
        format(self.name, self._id))
        # Code responsible for recovering the camera e.g. moving the head
        rospy.sleep(3)
        return FTSMTransitions.DONE_RECOVERING

    def configuring(self):
        rospy.loginfo('[{}][{}] Now I am reconfiguring the head RGBD camera by resetting the message bus'.
        format(self.name, self._id))
        # Code responsible for reconfiguring the camera e.g. resetting the message bus 
        rospy.sleep(5)
        return FTSMTransitions.DONE_CONFIGURING
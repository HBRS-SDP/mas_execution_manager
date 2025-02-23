#!/usr/bin/python3

import rospy
from pyftsm.ftsm import FTSMTransitions
from component_sm_base import ComponentSMBase
from sensor_msgs.msg import PointCloud2
from jsonschema import validate, ValidationError
import json
import numpy as np


class RGBDCameraSM(ComponentSMBase):
    """
    A class used to implement RGBD camera as fault tolerant component.

    ...

    Attributes
    ----------
    component_id : str,
        Unique id of the component

    monitor_manager_id : str,
        Unique id of the monitor manager

    nans_threshold : float
        Percentage threshold of NaN values (in the pointcloud) that is acceptable.
        If number of NaN values exceeds this limit, the component starts recovery behaviour.

    data_input_topic : str
        Name of the topic for obtaining the point cloud 
                 
    data_output_topic : str
        Name of the topic for outputting the point cloud
    
    monitoring_control_topic : str
        Name of the topic used to switch off and on the monitors

    monitoring_pipeline_server : str
        Address and port of the server used to communicate with the component monitoring 
        e.g. default address of the Kafka server is 'localhost:9092'

    monitors_ids : list[str]
        List of the unique ids of the monitors that are monitoring the current component

    general_message_format : dict
        Format of the message used to switch on and off monitors

    general_message_schema : dict
        Schema of the message used to switch on and off monitors

    monitoring_message_schema : dict
        Schema of the message received from the monitor as feedback
                
    data_transfer_timeout : int
        Time in seconds after which the feedback from the monitors is considered as not received

    max_recovery_attempts : int
        Maximum number of attempts to recover
    """
    def __init__(self, 
                 component_id,
                 monitor_manager_id,
                 storage_manager_id,
                 nans_threshold,
                 data_input_topic,
                 data_output_topic,
                 monitoring_control_topic,
                 monitoring_pipeline_server,
                 monitors_ids,
                 general_message_format,
                 general_message_schema,
                 monitoring_message_schema,
                 data_transfer_timeout,
                 max_recovery_attempts=1):
        super(RGBDCameraSM, self).__init__('RGBDCameraSM', 
                                           component_id=component_id,
                                           monitor_manager_id=monitor_manager_id,
                                           storage_manager_id=storage_manager_id,
                                           dependencies=[], 
                                           max_recovery_attempts=max_recovery_attempts,
                                           monitoring_control_topic=monitoring_control_topic,
                                           monitoring_pipeline_server=monitoring_pipeline_server,
                                           monitors_ids=monitors_ids,
                                           general_message_format=general_message_format,
                                           general_message_schema=general_message_schema,
                                           monitoring_message_schemas=[monitoring_message_schema],
                                           monitoring_timeout=5)
        
        self._pointcloud = None
        self._timeout = data_transfer_timeout
        self._nans_threshold = nans_threshold
        self._no_feedback_counter = 0
        self._ftsm_transition = None
        self._monitoring_database_connection_established = False

        self.__init_data_pipeline(input_topic=data_input_topic,
                                  output_topic=data_output_topic)

        self._last_active_time = rospy.Time.now()

    def __init_data_pipeline(self, input_topic: str, output_topic: str):
        '''
        Initialising publisher and listener for the transfer of the pointcloud.

            Parameters:
                input_topic (str) : Name of the topic for obtaining the point cloud 
                output_topic (str) : Name of the topic for outputting the point cloud
        '''
        # ROS poincloud listener
        self._pointcloud_listener = \
            rospy.Subscriber(
                input_topic, 
                PointCloud2,
                self.__callback
                )

        # ROS pointcloud publisher
        self._pointcloud_publisher = \
            rospy.Publisher(
                output_topic, 
                PointCloud2, 
                queue_size=10
                )

    def __callback(self, data: PointCloud2):
        '''
        Callback responsible for receiving ROS messages with pointcloud data.

            Parameters:
                data (PointCloud2): Object containing pointcloud data
        '''
        # Receiving pointcloud
        self._last_active_time = rospy.Time.now()
        #self.execution_requested = True
        self._pointcloud = data

        # Publishing poincloud to component monitoring
        self._pointcloud_publisher.publish(self._pointcloud)

    def handle_monitoring_feedback(self) -> str:
        '''
        Function for handling messages from the monitors responsible for monitoring the current component.

            Returns:
                str: State of the Fault Tolerant State Machine 
        '''
        time_now = rospy.Time.now()
             
        last_message = None

        try:
            if self._monitor_feedback_listener:
                for message in self._monitor_feedback_listener:
                    # Validate the correctness of the message
                    validate(instance=message.value, schema=self._monitoring_message_schemas[0])
                    
                    last_message = message

                    time_now = rospy.Time.now()
                    if not time_now - rospy.Duration(self._timeout) < self._last_active_time:
                        break

                    if message.value['healthStatus']['nan_ratio'] > self._nans_threshold:
                        rospy.logerr('[{}][{}] Received poincloud contains too many NaN values.'.
                        format(self.name, self._id))
                        return FTSMTransitions.RECOVER
                    else:
                        rospy.loginfo('[{}][{}] Received poincloud contains acceptable number of NaN values.'.
                        format(self.name, self._id))
                
                if last_message is None:
                    # Count to three and try to turn on the monitoring one more time then
                    if self._no_feedback_counter >= 3:
                        rospy.logwarn('[{}][{}] Trying to turn on the monitoring and storage one more time.'.
                        format(self.name, self._id))
                        if self.turn_on_monitoring() and self.turn_on_storage():
                            self._no_feedback_counter = 0
                    else:
                        self._no_feedback_counter += 1
                        rospy.logwarn('[{}][{}] No feedback from the monitoring.'.
                        format(self.name, self._id))

        except ValidationError:
            rospy.logwarn('[{}][{}] Invalid format of the feedback message from the monitor.'.
            format(self.name, self._id))

    def operation_without_monitoring(self):
        '''
        Function specyfing the behaviour of the component when the monitoring can not be anbled. 
        '''
        rospy.loginfo('[{}][{}] Component operates without monitoring.'.
            format(self.name, self._id))
        rospy.sleep(2)

    def operation_with_monitoring(self):
        '''
        Function specyfing the behaviour of the component when the monitoring can be anbled. 
            
            Returns:
                str: State of the Fault Tolerant State Machine         
        '''
        return self.handle_monitoring_feedback() 

    def running(self):
        '''
        Method for the behaviour of a component during active operation.

            Returns:
                str: State of the Fault Tolerant State Machine 
        '''
        
        time_now = rospy.Time.now()

        # If the poincloud is available
        if self._pointcloud:
            if self._is_kafka_available and \
                self._ftsm_transition is not FTSMTransitions.RECOVER:

                while not self.turn_on_monitoring():
                    rospy.sleep(2)

                while not self.turn_on_storage():
                    rospy.sleep(2)

                self._monitoring_database_connection_established = True

            # If the pointcloud is being continuously refreshed
            while time_now - rospy.Duration(self._timeout) < \
            self._last_active_time and self._pointcloud.data:
                time_now = rospy.Time.now()
                
                if self._is_kafka_available:
                    self._ftsm_transition = self.operation_with_monitoring()
                    if self._ftsm_transition == FTSMTransitions.RECOVER:
                        return FTSMTransitions.RECOVER
                else:
                    self.operation_without_monitoring()

        # If the poincloud is not available
        if time_now - rospy.Duration(self._timeout) >= self._last_active_time or \
            not self._pointcloud:
            rospy.logerr('[{}][{}] Can not receive the poincloud from head RGBD Camera.'.
            format(self.name, self._id))

            self._pointcloud = None
            
            if self._is_kafka_available and \
                self._monitoring_database_connection_established:

                self.turn_off_monitoring() 
                self.turn_off_storage()
                self._monitoring_database_connection_established = False
                self._ftsm_transition = FTSMTransitions.RECONFIGURE
            return FTSMTransitions.RECONFIGURE
        else:
            return self._ftsm_transition 

    def recovering(self):
        '''
        Method for component recovery.

            Returns:
                str: State of the Fault Tolerant State Machine 
        '''
        rospy.loginfo('[{}][{}] Now I am recovering the RGBD CAMERA by moving the head'.
        format(self.name, self._id))

        # Code responsible for recovering the camera e.g. moving the head
        rospy.sleep(3)
        # Restart kafka consumer to get only the newest feedback
        self._monitor_feedback_listener.close()
        self.init_monitor_feedback_listener()

        return FTSMTransitions.DONE_RECOVERING

    def configuring(self):
        '''
        Method for component configuration/reconfiguration.

            Returns:
                str: State of the Fault Tolerant State Machine 
        '''
        rospy.loginfo('[{}][{}] Now I am reconfiguring the head RGBD camera by resetting the message bus'.
        format(self.name, self._id))

        # Code responsible for reconfiguring the camera e.g. resetting the message bus 
        rospy.sleep(3)

        return FTSMTransitions.DONE_CONFIGURING
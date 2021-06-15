import rospy
import actionlib
from action_states import StreamRGBDSM
import json
import yaml

if __name__ == '__main__':
    rospy.init_node('stream_pointcloud')
    monitoring_message_schema = json.load(open('monitoring.schema'))
    general_message_format = json.load(open('general.json'))
    config = yaml.safe_load(open('config.yaml'))

    stream_pointcloud = StreamRGBDSM(**config, 
    monitoring_message_schema=monitoring_message_schema,
    general_message_format=general_message_format)
    
    try:
        stream_pointcloud.run()
        while stream_pointcloud.is_running and not rospy.is_shutdown():
            rospy.spin()
    except (KeyboardInterrupt, SystemExit):
        print('{0} interrupted; exiting...'.format(stream_pointcloud.name))
        stream_pointcloud.stop()
import rospy
import actionlib
from action_states import StreamRGBDSM

if __name__ == '__main__':
    rospy.init_node('stream_pointcloud')
    stream_pointcloud = StreamRGBDSM()
    try:
        stream_pointcloud.run()
        while stream_pointcloud.is_running and not rospy.is_shutdown():
            rospy.spin()
    except (KeyboardInterrupt, SystemExit):
        print('{0} interrupted; exiting...'.format(stream_pointcloud.name))
        stream_pointcloud.stop()
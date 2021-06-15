import rospy
import actionlib
from action_states import StreamRGBDSM
import json
import yaml

if __name__ == '__main__':
    rospy.init_node('stream_pointcloud')

    general_message_format = json.load(open('general.json'))
    general_message_schema = json.load(open('general.schema'))
    monitoring_message_schema = json.load(open('monitoring.schema'))
    config = yaml.safe_load(open('config.yaml'))

    stream_pointcloud = StreamRGBDSM(
        component_id=config['id'],
        data_input_topic=config['data_input_topics'][0],
        data_output_topic=config['data_output_topics'][0],
        monitoring_control_topic=config['monitoring']['control_topic'],
        monitoring_pipeline_server=config['monitoring']['pipeline_server'],
        monitor_feedback_topic=config['monitoring']['monitors'][0]['feedback_topic'],
        monitor_id=config['monitoring']['monitors'][0]['id'],
        general_message_format=general_message_format,
        general_message_schema=general_message_schema,
        monitoring_message_schema=monitoring_message_schema,
        data_transfer_timeout=config['data_transfer_timeout']
    )
    
    try:
        stream_pointcloud.run()
        while stream_pointcloud.is_running and not rospy.is_shutdown():
            rospy.spin()
    except (KeyboardInterrupt, SystemExit):
        print('{0} interrupted; exiting...'.format(stream_pointcloud.name))
        stream_pointcloud.stop()
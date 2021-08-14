import rospy
from rgbd_camera_sm import RGBDCameraSM
import json
import yaml

if __name__ == '__main__':
    rospy.init_node('stream_pointcloud')

    request_message_format = json.load(open('../schemas/request.json'))
    request_message_schema = json.load(open('../schemas/request.schema'))
    broadcast_message_schema = json.load(open('../schemas/broadcast.schema'))
    response_message_schema = json.load(open('../schemas/response.schema'))
    monitoring_message_schema = json.load(open('../schemas/monitoring.schema'))
    config = yaml.safe_load(open('../config/config.yaml'))

    stream_pointcloud = RGBDCameraSM(
        component_id=config['id'],
        monitor_manager_id=config['monitoring']['monitor_manager_id'],
        nans_threshold=config['threshold'],
        data_input_topic=config['data_input_topics'][0],
        data_output_topic=config['data_output_topics'][0],
        monitoring_control_topic=config['monitoring']['control_topic'],
        monitoring_pipeline_server=config['monitoring']['pipeline_server'],
        monitors_ids=[config['monitoring']['monitors'][0]['id']],
        request_message_format=request_message_format,
        request_message_schema=request_message_schema,
        respone_message_schema=response_message_schema,
        broadcast_message_schema=broadcast_message_schema,
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
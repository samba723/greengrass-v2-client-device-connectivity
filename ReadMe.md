# Create Greengrass V2 core using AWS IoT Core
![Greengrass Core Created](/Screenshots/GreengrassCoreCreated.png)

# Create an IoT Thing and download certificate &  private key
![IoT Core Thing Created](/Screenshots/IoTCoreThingCreated.png)

# Installl python, pip and requirements on the device
    $ python3 -m pip install -r requirements.txt

# Updated client_device.py with Greengrass discovery and establish connection with Greengrass core
    import time, logging
    import json, argparse
    from datetime import datetime
    from awscrt import io, http
    from awscrt.mqtt import QoS
    from awsiot import mqtt_connection_builder
    import requests

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--thingName", help = "Thing name")
    parser.add_argument("-c","--cert",help="Device certificate path")
    parser.add_argument("-k","--key",help="Device private key")
    parser.add_argument("-r","--region",help="AWS region")
    parser.add_argument("-st","--sub_topic",help="Subscription topic name",default="sample/sub/topic")
    parser.add_argument("-pt","--pub_topic",help="Publish topic name",default="sample/pub/topic")
    parser.add_argument("-m","--message",help="Message for publishing",default="hello device connectivity")
    parser.add_argument("-mc","--message_count",help="Message count",default=20)

    args = parser.parse_args()

    thing_name=args.thingName
    region=args.region
    discovery_url='https://greengrass-ats.iot.'+region+'.amazonaws.com:8443/greengrass/discover/thing/'+thing_name
    device_cert_file_name=args.cert
    device_key_file_name=args.key
    cert_key=(device_cert_file_name,device_key_file_name)
    discovery_req=requests.get(discovery_url,cert=cert_key)

    discovery_res=discovery_req.json()
    GG_core_thing=discovery_res['GGGroups'][0]['Cores'][0]['thingArn']
    group_CA=discovery_res['GGGroups'][0]['CAs'][0].encode('utf-8')
    connectivity_info=discovery_res['GGGroups'][0]['Cores'][0]['Connectivity']

    def exitDeviceClient(message):
        logging.error(message)
        exit(0)

    def on_connection_interupted(connection, error, **kwargs):
        logging.error('connection interrupted with error {}'.format(error))


    def on_connection_resumed(connection, return_code, session_present, **kwargs):
        logging.info('connection resumed with return code {}, session present {}'.format(return_code, session_present))

    def connectToGGCore():
        for host in connectivity_info:
            logging.info(f"Trying to connect to core {GG_core_thing} at host IP address {host['HostAddress']} and port {host['PortNumber']}")
            try: 
                mqtt_connection = mqtt_connection_builder.mtls_from_path(
                                        endpoint=host['HostAddress'],
                                        port=host['PortNumber'],
                                        cert_filepath=device_cert_file_name,
                                        pri_key_filepath=device_key_file_name,
                                        ca_bytes=group_CA,
                                        on_connection_interrupted=on_connection_interupted,
                                        on_connection_resumed=on_connection_resumed,
                                        client_id=thing_name,
                                        clean_session=False,
                                        keep_alive_secs=30)
                connect_future = mqtt_connection.connect()
                connect_future.result()
                logging.info('Client device connected to Greengrass core!')
                return mqtt_connection

            except Exception as e:
                logging.warning('Connection failed with exception {}'.format(str(e)))
                continue
        exitDeviceClient("All client connection attempts are failed")

    mqtt_connection=connectToGGCore()

    pub_topic=args.pub_topic
    sub_topic=args.sub_topic
    message=args.message
    messageCount=int(args.message_count)

    def on_publish(topic, payload, dup, qos, retain, **kwargs):
        logging.info('Message received on topic {} and payload is {}'.format(topic,payload))

    if sub_topic is not None:
        subscribe_future, packetId = mqtt_connection.subscribe(sub_topic, QoS.AT_MOST_ONCE, on_publish)
        subscribe_result = subscribe_future.result()
        logging.info('Subscription result {}'.format(str(subscribe_result)))

    if pub_topic is not None:
        payload={"message":message}
        loop=0
        while loop < messageCount:
            dt=datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
            payload["timestamp"]=dt
            payload["sequence"]=loop
            loop+=1
            payloadJson=json.dumps(payload)
            pub_future, packetId = mqtt_connection.publish(pub_topic, payloadJson, QoS.AT_MOST_ONCE)
            pub_future.result()
            logging.info('Published message to topic {} and message is {}, packet Id {}'.format(pub_topic, payloadJson,packetId))
            time.sleep(30)

# Add IoT Thing created previously to the client devices of the Greengrass core
![Associated Client Device To Greengrass](/Screenshots/AssociatedClientDeviceToGreengrass.png)

# Deployment Greengrass core Client Auth and other necessary components
![Client Devices Component Deployment](/Screenshots/ClientDevicesComponentDeployment.png)

# ClientDevice Auth component merge configuration
    {
        "deviceGroups": {
        "formatVersion": "2021-03-05",
        "definitions": {
            "MyDeviceGroup": {
            "selectionRule": "thingName: <thing-name>",
            "policyName": "ClientDevicePolicy"
            }
        },
        "policies": {
            "ClientDevicePolicy": {
            "AllowConnect": {
                "statementDescription": "Allow client devices to connect.",
                "operations": [
                "mqtt:connect"
                ],
                "resources": [
                "*"
                ]
            },
            "AllowPublish": {
                "statementDescription": "Allow client devices to publish to all topics.",
                "operations": [
                "mqtt:publish"
                ],
                "resources": [
                "*"
                ]
            },
            "AllowSubscribe": {
                "statementDescription": "Allow client devices to subscribe to all topics.",
                "operations": [
                "mqtt:subscribe"
                ],
                "resources": [
                "*"
                ]
            }
            }
        }
        }
    }

# Start the device client program created previously
    $ python3 client_device.py -t <thing-name> -c <device-certificat-path> -k <device-private-key-path> \
    -r <aws-region> -st <subscription-topic> -pt <publish-topic> -m <message> -mc <message-count>

# Client device program logs
![Client Device Logs](/Screenshots/ClientDeviceLogs.png)

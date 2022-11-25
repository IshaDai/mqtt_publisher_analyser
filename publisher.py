
import time
import paho.mqtt.client as mqtt

qos_pool = [0,1,2] # QoS level (0, 1 or 2)
delay_pool = [0, 10, 20, 50, 100, 500] # Specific delay (0ms, 10ms, 20ms, 50ms, 100ms, 500ms)
final_qos = 0 # Initial qos, can be changed 
final_delay = 500 # Initial delay, can be changed
counter = 0 # An incrementing counter which counts from 0.


def on_connect(client, userdata, flags, rc):
    """
    A default function of paho, used to connect the broker.
    And listen to these subscribed topics.

    :param client(mqtt.Client): The mqtt client used
    :param rc(int): The number shows the connect state
    """
    print("connected to the broker, state: " + str(rc))# Check the connection state.

    # Subscribe topics we want to listen.
    client.subscribe("request/qos")
    client.subscribe("request/delay")
    # client.subscribe("$SYS/#")
    return

def on_message(client, userdata, msg):
    """
    A default function of paho, used to receive messages from broker.
    Also it contatins the main function of Controller which used to change the publisher's behavior.
    Controller approach: The publisher will receive the message of changing qos and delay 
    sent by the analyser, and then enter the event processing flow. The controller will first 
    determine whether the acquired qos and delay belong to the scope of the assignment requirement. 
    If they do, it will change the global variables to the new qos and delay, thereby changing the 
    behavior of the publisher, and the previous publisher process will be stopped (updated).
    
    :param msg(str):The received message, which has topic, qos, payload, and retain
    """
    if(msg.topic == "request/qos"):
        global final_qos
        final_qos = controller(int(msg.payload),qos_pool) 
        print("receive new qos: " + str(msg.payload))
    elif(msg.topic == "request/delay"):
        global final_delay
        final_delay = controller(int(msg.payload),delay_pool)
        print("receive new delay: " + str(msg.payload))
    else:
        print("receive " + str(msg.topic) + ": " + str(msg.payload))
    return

def controller(payload,pool):
    """
    Confirm whether the qos/delay should be changed.
    
    :param payload(int):The received message's payload, should be an int number.
    :param pool(list):The specific numbers for qos and delay to use.
    :return: The new payload received.
    """
    if(payload in pool):
        return payload 

def new_sleep(delay):
    """
    Let the publisher sleep according to the delay time.
    The reason for using this function is that general time.sleep() has a
    big time deviation for some computers(such as mine, system is Win10).
    
    :param delay: The delay time for publishing messages"""
    delay_ms = delay/1000 # The unit of delay is second, replaced by millisecond.
    # If delay is more than 0ms, can reduce the time deviation.
    if(delay_ms>0):
        start_time = time.time()
        while True:
            cur_time = time.time()
            if cur_time - start_time > delay_ms:
                break
    # If delay is 0ms, don't perform any sleep funciton.
    else:
        pass
    return

# Set the client_id, username, and password we want to use.
client = mqtt.Client(client_id="3310-William")
client.username_pw_set(username="student",password="33102021")
# Subscribe the topics and listen to them.
client.on_connect = on_connect
client.on_message = on_message
# Enter the IP Addresss and port to connect to a broker.
input_host = input("Please enter the IP address: ")
input_port = input("Please enter the port: ")
print("")
client.connect(input_host, int(input_port))

# Publish messages continuously.
client.loop_start()
while True:
    topic = "counter/"+ str(final_qos) + "/" + str(final_delay)
    client.publish(topic = topic, payload = counter, qos = final_qos, retain=False)
    print("publish a message: " + topic + " " + str(counter))
    counter += 1
    new_sleep(final_delay)


    
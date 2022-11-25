import time
import paho.mqtt.client as mqtt
import csv

qos = None
delay = None
counter = 0 # Count how many messages received during the run_time.
sys_lst = [] # List used to keep the $SYS messages
msg_lst = [] # List used to keep the msseage payload.
time_lst = [] # List used to keep the time of receipt.
run_time = 120 # Time in seconds, how long to listen to the messages.


def on_connect(client, userdata, flags, rc):
    """
    A default function of paho, used to connect the broker.
    And listen to these subscribed topics.

    :param client(mqtt.Client): The mqtt client used
    :param rc(int): The number shows the connect state
    """
    print("connected to the broker, state: " + str(rc))# Check the connection state.

    # Subscribe topics we want to listen.
    client.subscribe("counter/" + str(qos) + "/" + str(delay))# listen to a topic with input qos and delay
    # client.subscribe("$SYS/brokers/emqx@10.65.190.170/metrics/messages/delayed")
    # client.subscribe("$SYS/brokers/emqx@10.65.190.170/metrics/messages/dropped")
    # client.subscribe("$SYS/brokers/emqx@10.65.190.170/metrics/messages/sent")
    # client.subscribe("$SYS/brokers/emqx@10.65.190.170/metrics/client/connected")
    # client.subscribe("$SYS/brokers/emqx@10.65.190.170/metrics/messages/published")
    # client.subscribe("$SYS/#")
    client.subscribe("$SYS/broker/clients/active")
    client.subscribe("$SYS/broker/load/messages/received/1min")
    client.subscribe("$SYS/broker/load/messages/sent/1min")
    client.subscribe("$SYS/broker/heap/current size")
    client.subscribe("$SYS/broker/heap/maximum size")
    return

def on_message(client, userdata, msg):
    """
    A default function of paho, used to receive messages from broker.
    Create a csv file and record the message received during the running time.
    Also, add received message data into lists for the follow-up statistics and analysis.
    
    :param msg(str):The received message, which has topic, qos, payload, and retain
    """
    global counter
    if(msg.topic == "counter/" + str(qos) + "/" + str(delay)):
        # Create a csv file and write message into it.
        filename = "record_"+str(qos)+"_"+str(delay)+".csv"
        with open(filename,"a") as f:
            writer = csv.writer(f)
            writer.writerow([msg.topic,int(msg.payload)])
        counter += 1 # count how many counter/*/* messages received during the running time.
        msg_lst.append(int(msg.payload)) # append each message payloads into the msg_lst.
        time_lst.append((time.time())) # append the time of each receipt into the time_list.
    else:
        print(str(msg.topic) + ":" + str(msg.payload))
        sys_lst.append((msg.topic,msg.payload))
    return


def message_rate(counter, time):
    """
    Calculate the overall average rate of messages actually receive across the period.
    
    :param counter(int): The number of how many messages received during the period
    :param time(int): The time (in second) of the period
    """
    msg_rate = counter/time
    print("The overall average rate of messages is: " + str(msg_rate) + "packets/s")
    return 

def loss_rate(counter, all_msg):
    """
    Calculate the rate of message loss.
    
    :param counter(int): The number of how many messages received during the period
    :param all_msg(list): The list contains the message payloads
    """
    packets_num = max(all_msg) - min(all_msg)
    rate = 1 - counter/(packets_num + 1) # 100 - 1 = 99 but there are 100 messages, so plus 1.
    print("The rate of message loss is: " + str(rate*100) + "%")
    return

def out_order_rate(counter, all_msg):
    """
    Calculate the rate of any out-of-order messages.
    
    :param counter(int): The number of how many messages received during the period
    :param all_msg(list): The list contains the message payloads
    """
    out_of_order_num = 0
    for index in range(len(all_msg)):
        # If the message is not equal to the pervious one plus 1, it is an out-of-order message.
        if all_msg[index] != all_msg[index-1]+1:
            out_of_order_num += 1
    rate = out_of_order_num / counter
    print("The rate of any out-of-order messages is: " + str(rate*100) + "%")
    return

def mean_gap(counter,time_lst):
    """
    Calculate the mean inter-message-gap.
    
    :param counter(int): The number of how many messages received during the period.
    :param time_lst(list): The list contains the time of reciept of each message.
    """
    time_used = max(time_lst)-min(time_lst) # the actual time used.
    mean = time_used/counter    
    print("The mean inter-message-gap: " + str(mean*1000) + "ms")
    return

def median_gap(msg_lst,time_lst):
    """
    Calculate the median inter-message-gap.
    
    :param counter(int): The number of how many messages received during the period.
    :param time_lst(list): The list contains the time of reciept of each message.
    """
    diff_lst = []
    for index in range(1,len(time_lst)):
        # If messages are ordered, put them into a new list.
        if(msg_lst[index] - 1 == msg_lst[index - 1]):
            diff_lst.append(time_lst[index] - time_lst[index-1])
    
    median_list = sorted(diff_lst) # Sort list elements from small to large.
    mid_position = len(diff_lst)//2 #round down
    # If the length of sorted list is even.
    if len(median_list)%2 == 0:
        median = (median_list[mid_position] + median_list[mid_position+1])/2
        print("The median inter-message-gap: " + str(median*1000) + "ms")
    # If the length of sorted list is odd.
    else:
        median = median_list[mid_position]
        print("The median inter-message-gap: " + str(median*1000) + "ms")
    return

def analyser():
    """
    This function contains all defined calculation functions above,
    and show the statistical result of each combination of qos and delay.
    """
    global counter
    message_rate(counter, run_time)    
    loss_rate(counter, msg_lst)
    out_order_rate(counter, msg_lst)
    mean_gap(counter,time_lst)
    median_gap(msg_lst,time_lst)
    counter = 0
    msg_lst.clear()
    time_lst.clear()
    sys_lst.clear()
    print("")
    return

# Set the client_id, username, and password we want to use.
client = mqtt.Client(client_id="3310-Wenpei")
client.username_pw_set(username="student",password="33102021")

# Enter the IP Addresss and port to connect to a broker.
input_host = input("Please enter the IP address: ")
input_port = input("Please enter the port: ")


while True:
    '''
    Manually enter the qos and delay that we want to listen to, and then we will subscribe to the topic 
    and start receiving. The program will run for the specific time (2 minutes in the assignment),
    unsubscribe from the topic after the time is over, and analyse the information collected in the past 
    two minutes. 
    After the analysis results are printed out, the user can directly input the qos and delay that they
    wants to listen to without restarting the program. If user enter 3310 or press Enter when entering qos 
    or delay, the program will end.
    '''
    # Enter the qos we want to pass to the publisher, enter 3310 to end the process.
    qos = int(input("Please enter the qos(enter 3310 for quit): "))
    if(qos == 3310):
        print("quit the program")
        break    
    # Enter the delay we want to pass to the publisher, enter 3310 to end the process.
    delay = int(input("Please enter the delay(enter 3310 for quit): "))
    if(delay == 3310):
        print("quit the program")
        break
    else:
        # Subscribe the topics and listen to them.
        client.on_connect = on_connect
        client.on_message = on_message
        # Connect to the broker.
        client.connect(input_host, int(input_port))
        client.loop_start()
        client.publish('request/qos', payload=qos,qos=2)
        client.publish('request/delay', payload=delay,qos=2)
        print("published successfully, now the qos and delay change to:" + str(qos)+ "/" + str(delay))
        print("run for " + str(run_time) + "seconds")
        # Run for 120 mins to collect the data
        time.sleep(run_time)
        # Unsubscribe the topic after 2 mins.
        client.unsubscribe("counter/" + str(qos) + "/" + str(delay))
        # Analyse the data we collected.
        analyser()
        #print(sys_lst)    
import socket
import threading
import random
# import queue
from datetime import datetime

HEADER = 64
FORMAT = 'utf-8'
PORT = 80

NULL_MSG = "null_msg"
NULL_IP = "null_ip"
ITEM_DEF = [NULL_MSG, 0, NULL_IP]
LIST_INIT = [ITEM_DEF]

FIRST_TASK_IN_SERVER = 1
INDEX_OF_IP = 2
INDEX_OF_MSG = 0

LB_SERVER_SERVER_SIDE_IP = "10.0.0.1"
LB_SERVER_CLIENT_SIDE_IP = "192.168.0.1"

LB_ADDR_SERVER_SIDE = (LB_SERVER_SERVER_SIDE_IP, PORT)

# Setting up


print("%s: LB Started-----" % (datetime.now().strftime("%H:%M:%S")))

print("%s: Connecting to servers-----" % (datetime.now().strftime("%H:%M:%S")))

serv0 = "192.168.0.100"
ADDR_0 = (serv0, PORT)

serv0_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv0_socket_connection.connect(ADDR_0)

serv1 = "192.168.0.101"
ADDR_1 = (serv1, PORT)

serv1_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv1_socket_connection.connect(ADDR_1)

########### serv2
serv2 = "192.168.0.102"
ADDR_2 = (serv2, PORT)

serv2_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv2_socket_connection.connect(ADDR_2)

########### serv3
serv3 = "192.168.0.103"
ADDR_3 = (serv3, PORT)

serv3_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv3_socket_connection.connect(ADDR_3)

########### serv4
serv4 = "192.168.0.104"
ADDR_4 = (serv4, PORT)

serv4_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv4_socket_connection.connect(ADDR_4)

########### serv5
serv5 = "192.168.0.105"
ADDR_5 = (serv5, PORT)

serv5_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv5_socket_connection.connect(ADDR_5)

########### serv6
serv6 = "192.168.0.106"
ADDR_6 = (serv6, PORT)

serv6_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv6_socket_connection.connect(ADDR_6)

########### serv7
serv7 = "192.168.0.107"
ADDR_7 = (serv7, PORT)

serv7_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv7_socket_connection.connect(ADDR_7)

########### serv8
serv8 = "192.168.0.108"
ADDR_8 = (serv8, PORT)

serv8_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv8_socket_connection.connect(ADDR_8)

########### serv9
serv9 = "192.168.0.109"
ADDR_9 = (serv9, PORT)

serv9_socket_connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv9_socket_connection.connect(ADDR_9)

###
# just for keeping score of which host is address.
hosts_ip = ["10.0.0.101", "10.0.0.102", "10.0.0.103", "10.0.0.104", "10.0.0.105"]

servers_ip = ["192.168.0.100", "192.168.0.101", "192.168.0.102", "192.168.0.103", "192.168.0.104", "192.168.0.105",
              "192.168.0.106", "192.168.0.107", "192.168.0.108", "192.168.0.109"]
servers_sockets_connections = [serv0_socket_connection, serv1_socket_connection, serv2_socket_connection,
                               serv3_socket_connection, serv4_socket_connection, serv5_socket_connection,
                               serv6_socket_connection, serv7_socket_connection, serv8_socket_connection,
                               serv9_socket_connection]
servers_types = ["V", "V", "V", "V", "V", "V", "M", "M", "M", "M"]

servers_num_of_waiting_answers = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
# server in place i returned an answer.
# it's for hosts that are waiting for an answer, and got an answer but not for the current thread host.

servers_work_load = []
servers_work_load_num_of_current_tasks = []
for x in range(10):
    servers_work_load.append(LIST_INIT)
    servers_work_load_num_of_current_tasks.append(0)


# each item is ["M1", <time for server to do the request> ,"<host_ip>"] when item i is server i.

# Functions

def time_to_process_req_for_server(type_of_server, type_of_req, digit_input):
    if (type_of_req == type_of_server):
        # Music server is "M"
        # Video server is "V"
        return digit_input

    if (type_of_server == "M"):
        if (type_of_req == "V"):
            return digit_input * 3
        else:  # req is "P"
            return digit_input * 2

    # else :# (type_of_server == "V"):
    if (type_of_req == "M"):
        return digit_input * 2
    else:  # req is "P"
        return digit_input


# returns an index of a chosen server
def get_a_server(type_of_msg, req_time_str):
    arr = random.sample([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 2)

    i1_pick_rand_server = arr[0]
    i2_pick_rand_server = arr[1]

    servA_type = servers_types[i1_pick_rand_server]
    servB_type = servers_types[i2_pick_rand_server]

    time_to_process_new_req_servA = time_to_process_req_for_server(servA_type, type_of_msg, int(req_time_str))
    time_to_process_new_req_servB = time_to_process_req_for_server(servB_type, type_of_msg, int(req_time_str))

    threadLock.acquire()
    time_for_current_tasks_in_A = get_total_time_current(i1_pick_rand_server)
    threadLock.release()

    threadLock.acquire()
    time_for_current_tasks_in_B = get_total_time_current(i2_pick_rand_server)
    threadLock.release()



    total_time_todo_task_forA = time_to_process_new_req_servA + time_for_current_tasks_in_A
    total_time_todo_task_forB = time_to_process_new_req_servB + time_for_current_tasks_in_B

    # choosing a server
    if (total_time_todo_task_forA <= total_time_todo_task_forB):
        i_serv_to_send = i1_pick_rand_server
    else:
        i_serv_to_send = i2_pick_rand_server
    return i_serv_to_send


def get_total_time_current(i_server):
    sum = 0
    for task in servers_work_load[i_server]:
        sum += task[1]
        # print(task[1])
    return sum


# time_to_process is the type for server i with his type to do this msg.
def add_task_to_server_load(i_server, msg, time_to_process, host_ip):
    servers_work_load[i_server].append([msg, time_to_process, host_ip])
    servers_work_load_num_of_current_tasks[i_server] += 1


def remove_task_from_host_work_load(i_server):
    servers_work_load[i_server].pop(1)
    servers_work_load_num_of_current_tasks[i_server] -= 1


def get_server_top_host_ip(i_server):
    return servers_work_load[i_server][FIRST_TASK_IN_SERVER][INDEX_OF_IP]


def get_server_top_msg(i_server):
    return servers_work_load[i_server][FIRST_TASK_IN_SERVER][INDEX_OF_MSG]


def handle_client(conn, addr):
    # message from the host:
    recv_msg = conn.recv(HEADER).decode(FORMAT)
    host_ip = addr[0]

    type_of_msg = recv_msg[0]
    req_time_str = recv_msg[1:len(recv_msg)]

    i_serv_to_send = get_a_server(type_of_msg, req_time_str)

    serv_choosen_connection_packet = servers_sockets_connections[i_serv_to_send]

    print("%s: " % (
            datetime.now().strftime(
                "%H:%M:%S") + " recieved request " + recv_msg + " from " + host_ip + ", sending to " + servers_ip[
                i_serv_to_send]))

    # Sending the packet and updating the load of the server.
    serv_choosen_connection_packet.send(recv_msg.encode('utf-8'))

    time_for_new_task = time_to_process_req_for_server(servers_types[i_serv_to_send], type_of_msg, int(req_time_str))

    threadLock.acquire()  # lock
    add_task_to_server_load(i_serv_to_send, recv_msg, time_for_new_task, host_ip)
    threadLock.release()  # unlock

    resv_msg2 = serv_choosen_connection_packet.recv(2048).decode('utf-8')

    threadLock.acquire()  # lock
    servers_num_of_waiting_answers[i_serv_to_send] += 1
    threadLock.release()  # unlock

    waiting_for_replay_from_server = True
    while waiting_for_replay_from_server:
        if (servers_num_of_waiting_answers[i_serv_to_send] > 0 and host_ip == get_server_top_host_ip(i_serv_to_send)):
            waiting_for_replay_from_server = False
            resv_msg2 = get_server_top_msg(i_serv_to_send)
            # if (recv_msg != resv_msg2):
            #     print("not the correct message!")

    threadLock.acquire()  # lock
    remove_task_from_host_work_load(i_serv_to_send)
    servers_num_of_waiting_answers[i_serv_to_send] -= 1
    threadLock.release()  # unlock

    conn.send(resv_msg2.encode(FORMAT))
    conn.close()


def start_listening():
    server.listen(5)
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


# Just so the thread is not dead!
def stay_connected_servers():
    while True:
        a = 1


threadLock = threading.Lock()  # for later when we update the servers queues, to avoid race conditions with the threads.

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(LB_ADDR_SERVER_SIDE)

thread = threading.Thread(target=stay_connected_servers, args=())
thread.start()
start_listening()

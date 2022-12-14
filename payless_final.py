'''
Team Roll nos. : 2022H1030111P,
Controller for PayLess

TODO Analyze flow removed- done
TODO Checkpointing for active probing and modified checkpointing for flowremoved stats
Notes:
    FlowStats will give SWITCH stats for ALL FLOWS
    FlowRemoved will give flow stats for ONE FLOW
'''

# cd X:\BITS PILANI\1-1\ACN\Lab stuff
# scp payless_final.py mininet@192.168.56.101:~/pox/pox/misc
# scp lab4_2_topo.py mininet@192.168.56.101:~/mininet/custom
# ./pox.py log.level --DEBUG misc.payless_final

from pox.core import core
from pox.lib.util import dpid_to_str
import pox.openflow.libopenflow_01 as of
from pox.lib.recoco import Timer
import time

log = core.getLogger()
start_time = 0
current_time = 0

# The active_list data structure is for storing the list of active flows at every switch
active_list = []
# The utilization_table data structure is for storing the list of checkpoints at every switch for every port
utilization_table = []
'''
The mac_to_port data structure at the controller stores the mapping between mac address and port for every switch.
It is actually a dictionary of dictionaries.
The key for the outer dictionary is the switch dpid. The value for the outer dictionary is another dictionary 
whose keys are mac addresses of hosts in the network.
The value for the inner dictionary is port through which we can reach the host having the mac address specified 
in the key from the switch mentioned in the key of outer dictionary.
eg: mac_to_port={5:{{40:10:40:10:40:10,2},{40:10:40:10:40:20,1}},4:{{40:10:40:10:40:10,6},{40:10:40:10:40:20,3}}}
In this example, first key of outer dictionary is 5. This is dpid of a switch. So, this means that to reach 
host with mac address 40:10:40:10:40:10 from switch 5, you have to forward to port 2.
To reach host with mac address 40:10:40:10:40:20 from switch 5, you have to forward to port 1.
To reach host with mac address 40:10:40:10:40:10 from switch 4, you have to forward to port 6
To reach host with mac address 40:10:40:10:40:20 from switch 4, you have to forward to port 3
'''
mac_to_port = {}
timeout_value = 0
hard_or_soft = 2
prv_utilization = {}

# Global variables for payless algorithm
schedule_table = dict()
started_timers = []  # Keeps track of timeout values for which timer has been triggered already
t_min = 5  # Minimum probe interval
t_max = 60  # Maximum probe interval
a = 1.5  # Factor to increase probe time
b = 1.5  # Factor to decrease probe time
delta_lower = 10000  # Lower traffic threshold for probing
delta_higher = 5000  # Upper traffic threshold for probing
total_probes = 0


def handle_timer_elapse(t_val, event):
    global total_probes
    log.debug("Current schedule table:")
    log.debug(schedule_table)
    log.debug("In handle_timer_elapse method for " + str(t_val) + " seconds bucket")
    if not schedule_table[t_val]:
        log.debug("Stopping timer for " + str(t_val) + " seconds bucket")
        return False
    msg = of.ofp_stats_request(body=of.ofp_flow_stats_request())
    total_probes += 1
    event.connection.send(msg)


def save_utilization_data(dpid, port, utilization):
    global current_time, prv_utilization
    prv_utilization[dpid][port]['time'] = current_time
    prv_utilization[dpid][port]['byte_count'] = utilization


# for entry in schedule_table:
# 	Timer(entry, handle_timer_elapse, args=[entry])

# log.debug("Sending message to switch %s", util.dpid_to_str(curSwitch))
# 					msg = of.ofp_stats_request(body=of.ofp_flow_stats_request())
# 					switches[curSwitch].connection.send(msg)

# fuction to create a timestamp and updating the current time
def get_the_time():
    global current_time
    flock = time.localtime()
    current_time = time.mktime(flock) - start_time
    then = "[%s-%s-%s" % (str(flock.tm_year), str(flock.tm_mon), str(flock.tm_mday))
    if int(flock.tm_hour) < 10:
        hrs = "0%s" % (str(flock.tm_hour))
    else:
        hrs = str(flock.tm_hour)
    if int(flock.tm_min) < 10:
        mins = "0%s" % (str(flock.tm_min))
    else:
        mins = str(flock.tm_min)
    if int(flock.tm_sec) < 10:
        secs = "0%s" % (str(flock.tm_sec))
    else:
        secs = str(flock.tm_sec)
    then += "]%s.%s.%s" % (hrs, mins, secs)
    return then


# function to add a new flow to the active_list data structure
def add_flow(event):
    global active_list, current_time
    flow = dict()
    flow['connection'] = event.connection
    flow['dpid'] = event.connection.dpid
    flow['in_port'] = event.port
    flow['dst'] = event.parsed.dst
    flow['active'] = 1
    flow['timestamp'] = get_the_time()
    flow['start_time'] = current_time
    flow['end_time'] = 0
    flow['stat_collect_timeout'] = t_min
    if flow not in active_list:
        if t_min not in schedule_table.keys():
            schedule_table[t_min] = [flow]
        else:
            schedule_table[t_min].append(flow)
        if t_min not in started_timers:
            Timer(t_min, handle_timer_elapse, args=[t_min, event], recurring=True)
            started_timers.append(t_min)
        log.debug("\nFlow added to schedule table. Current schedule table:\n")
        log.debug(schedule_table)
        log.debug("\n")
        active_list.append(flow)
        file1 = open("output.txt", "a")
        str1 = 'Flow added'
        log.debug(str1)
        file1.write(str1)
        file1.write("\n")
        str1 = str(flow)
        log.debug(str1)
        file1.write(str1)
        file1.write("\n")
        file1.close()
        return True
    else:
        return False


# function to add a new checkpoint to the utilization_table data structure
def add_checkpoint(event, matching_flow):
    global current_time, active_list, utilization_table
    checkpoint = dict()
    checkpoint['dpid'] = event.connection.dpid
    checkpoint['dst'] = event.ofp.match.dl_dst
    checkpoint['time'] = current_time
    checkpoint['flow_start_time'] = matching_flow['start_time']
    duration = matching_flow['end_time'] - matching_flow['start_time']
    checkpoint['utilization'] = ((event.ofp.byte_count * 8.0) / 1000) / duration  # storing utilization in kbps
    remaining = 0
    # check whether it is a partial utilization or not. If some flows are remaining, it is partial
    for flow in active_list:
        if checkpoint['dpid'] == flow['dpid'] and \
                checkpoint['dst'] == flow['dst'] and flow['active'] == 1 and flow['start_time'] < checkpoint['time']:
            remaining = remaining + 1
    checkpoint['active'] = remaining
    utilization_table.append(checkpoint)
    return checkpoint


'''
handle flowRemoved : 
Fired when a flow expires at a switch
'''


def _handle_FlowRemoved(event):
    global current_time, active_list, utilization_table
    timestamp = get_the_time()
    matching_flow = {}
    # find the matching flow in the active_list
    for flow in active_list:
        if flow['dpid'] == event.connection.dpid and flow['dst'] == event.ofp.match.dl_dst and \
                flow['in_port'] == event.ofp.match.in_port and flow['active'] == 1:
            flow['active'] = 0
            # Remove flow from schedule_table
            schedule_table[flow['stat_collect_timeout']].remove(flow)
            log.debug("Following flow has been removed from schedule table as FlowRemoved message "
                      "has been obtained for the same")
            log.debug(flow)
            log.debug("\nCurrent schedule table:\n")
            log.debug(schedule_table)
            log.debug("\n")
            flow['end_time'] = flow['start_time'] + event.ofp.duration_sec
            if event.idleTimeout:
                flow['end_time'] = flow['end_time'] - event.ofp.idle_timeout
                current_time = current_time - event.ofp.idle_timeout
            matching_flow = flow
            break
    if matching_flow['end_time'] - matching_flow['start_time'] > 0:
        checkpoint = add_checkpoint(event, matching_flow)
        file1 = open("output.txt", "a")
        str1 = "Time: " + str(timestamp) + " Checkpoint: " + str(
            current_time) + " Utilization on link from switch " + str(event.connection.dpid) + " through port " + str(
            mac_to_port[event.connection.dpid][event.ofp.match.dl_dst]) + " = " + str(
            checkpoint['utilization']) + " kbps\nFlows remaining: " + str(checkpoint['active'])
        log.debug(str1)
        file1.write(str1)
        file1.write("\n")
        # check for any previous checkpoints with partial utilization values
        for cp in utilization_table:
            if cp['dpid'] == event.connection.dpid and \
                    cp['dst'] == event.ofp.match.dl_dst and \
                    cp['active'] > 0 and cp['time'] < checkpoint['time']:
                cp['utilization'] = cp['utilization'] + checkpoint['utilization']
                cp['active'] = cp['active'] - 1
                str1 = "Checkpoint: " + str(cp['time']) + " Utilization on link from switch " + str(
                    event.connection.dpid) + " through port " + str(
                    mac_to_port[event.connection.dpid][event.ofp.match.dl_dst]) + " = " + str(
                    cp['utilization']) + " kbps Flows remaining: " + str(cp['active'])
                log.debug(str1)
                file1.write(str1)
                file1.write("\n")
        file1.close()


'''
handle ConnectionUp :
fired in response to the establishment of a new control channel with a switch.
'''


def _handle_ConnectionUp(event):
    global mac_to_port, start_time, prv_utilization
    if start_time == 0:
        start_time = time.mktime(time.localtime())
    # initialise the outer dictionary with key values as switch dpids as the connection with switch gets established
    # and with empty values
    mac_to_port[event.connection.dpid] = {}
    # initialise the outer dictionary of prv_utilization with key values as switch dpids as the connection
    # with switch gets established and with empty values
    prv_utilization[event.connection.dpid] = {}
    # log.debug("Switch ", event.connection.dpid)
    # log.debug(dpid_to_str(event.connection.dpid))
    for swprt in event.connection.features.ports:
        if swprt.port_no != 65534:
            log.debug(swprt.name)
            prv_utilization[event.connection.dpid][swprt.port_no] = {'time': 0, 'utilization': 0.0}


'''
handle packetIn : 
Fired when the controller receives an OpenFlow packet-in messagefrom a switch, 
which indicates that a packet arriving at a switch port has either failed to match all entries in the table, 
or the matching entry included an action specifying to send the packet to the controller.
'''


def _handle_PacketIn(event):
    global mac_to_port, timeout_value
    dpid = event.connection.dpid
    inport = event.port
    packet = event.parsed
    if not packet.parsed:
        log.warning("%i %i ignoring unparsed packet", dpid, inport)

    # We need to fill up the mac_to_port data structure as new packets come in
    # If a packet from a particular source comes to switch through a particular port, we know that in the future,
    # if we want to reach that particular source in the future, we can forward to this port
    # Store this information in mac_to_port if it's not already there
    if packet.src not in mac_to_port[dpid]:
        mac_to_port[dpid][packet.src] = event.ofp.in_port
    # If the packet's destination mac address is there in the inner dictionary corresponding to this switch,
    # add flow rule to forward the packet to the port in the dictionary
    if packet.dst in mac_to_port[dpid]:
        if add_flow(event):
            # flow_mod is for adding the flow rule
            msg = of.ofp_flow_mod()
            msg.match.in_port = inport
            msg.match.dl_dst = packet.dst
            msg.actions.append(of.ofp_action_output(port=mac_to_port[dpid][packet.dst]))
            if hard_or_soft == 1:
                msg.hard_timeout = timeout_value
            else:
                msg.idle_timeout = timeout_value
            msg.flags = of.OFPFF_SEND_FLOW_REM
            event.connection.send(msg)
            # The following lines are needed because the packet that causes the flow rule to be installed doesn't
            # actually follow the flow rules
            # Only the subsequent packets follow the flow rules
            # So in order for the first packet to not be dropped, the controller needs to explicitly the packet_out for
            # this packet to the same output port that the flow rule was installed
            msg = of.ofp_packet_out(data=event.ofp)
            msg.actions.append(of.ofp_action_output(port=mac_to_port[dpid][packet.dst]))
            event.connection.send(msg)
    # If the packet's destination mac address is not there in the inner dictionary corresponding to this switch,
    # we just flood the packet
    else:
        msg = of.ofp_packet_out(data=event.ofp)
        msg.actions.append(of.ofp_action_output(port=of.OFPP_ALL))
        event.connection.send(msg)

    # f = (event.connection, inport, t_min, 0)
    # if t_min in schedule_table.keys():
    #     schedule_table[t_min].append(f)
    # else:
    #     schedule_table[t_min] = [f]
    #     Timer(t_min, handle_timer_elapse, args=[t_min], recurring=True)


def _handle_flowstats_received(event):
    log.debug("Flowstats received from " + str(event.connection.dpid))
    global current_time, mac_to_port, prv_utilization, timer_value, utilization_table
    # timestamp = get_the_time()
    dpid = event.connection.dpid
    # utilization = 0
    # port = 0
    # iterate through every flow in the list and add up the utilization
    for f in event.stats:
        bytes = f.byte_count
        duration = f.duration_sec
        log.debug("bytes -> " + str(bytes))
        log.debug("duration -> " + str(duration))
        log.debug("delta_lower -> " + str(delta_lower))
        port = mac_to_port[dpid][f.match.dl_dst]
        # TODO need to check byte count PER FLOW

        # Getting previous utilization data as probe timer will be adjusted according to the difference in byte count
        # between previous and current utilization calculation
        prev_byte_count = prv_utilization[dpid][port]['byte_count']
        prev_util_time = prv_utilization[dpid][port]['time']

        diff_byte_count = bytes - prev_byte_count
        diff_duration = current_time - prv_utilization[dpid][port]['time']
        checkpoint = current_time

        for cp in utilization_table:
            if cp['dpid'] == event.connection.dpid and \
                    cp['dst'] == event.ofp.match.dl_dst and \
                    cp['active'] > 0:
                cp['time'] = current_time
                # and matching_flow['start_time'] < cp['time'] < checkpoint['time']:

        log.info("Difference in byte count = " + str(diff_byte_count))

        for flow in active_list:
            if flow['dpid'] == event.connection.dpid and flow['in_port'] == port:
                log.debug("Previous stat collection timeout: " + str(flow['stat_collect_timeout']))
                # Remove flow from previous timer bucket in schedule table
                schedule_table[flow['stat_collect_timeout']].remove(flow)
                if diff_byte_count < delta_lower:
                    log.debug("Increasing probing time for switch: " + str(dpid))
                    # Increase probe time as min(t_max, current probe time * a)
                    flow['stat_collect_timeout'] = min(t_max, flow['stat_collect_timeout'] * a)
                elif diff_byte_count > delta_higher:
                    log.debug("Decreasing probing time for switch: " + str(dpid))
                    # Decrease probe time as max(t_min, current probe time / b)
                    flow['stat_collect_timeout'] = max(t_min, flow['stat_collect_timeout'] / a)
                log.debug("New stat collection timeout: " + str(flow['stat_collect_timeout']))
                # if timer has not already been started for the new timer value
                if flow['stat_collect_timeout'] not in started_timers:
                    # create a new entry in schedule table with key as new timer value and value as the flow
                    schedule_table[flow['stat_collect_timeout']] = [flow]
                    # Append new timer value to started_timers list to denote that timer of this value has
                    # already been started
                    started_timers.append(flow['stat_collect_timeout'])
                    # Start timer with new timer value
                    Timer(t_min, handle_timer_elapse, args=[flow['stat_collect_timeout'], event], recurring=True)
                else:
                    # Append the flow to entry (list) with key as new timer value
                    schedule_table[flow['stat_collect_timeout']].append(flow)

        # u = ((bytes * 8.0) / 1000) / duration
        # utilization = utilization + u
        # port = mac_to_port[dpid][f.match.dl_dst]
    # if (current_time - prv_utilization[dpid][port]['time'] >= timer_value):
    #     file1 = open("output.txt", "a")
    #     str1 = "Active probing - Time: " + str(timestamp) + " Seconds: " + str(
    #         current_time) + " Utilization on link from switch " + str(dpid) + " through port " + str(
    #         port) + " = " + str(utilization) + " kbps"
    #     log.debug(str1)
    #     file1.write(str1)
    #     file1.write("\n")
    #     file1.close()
    #     save_utilization_data(dpid, port, utilization)


'''
launch :
Its the main method
'''


def launch():
    global timeout_value
    hard_or_soft = int(input("Enter 1 for hard timeout, 2 for soft timeout "))
    timeout_value = int(input("Enter the timeout value: "))
    core.openflow.addListenerByName("ConnectionUp", _handle_ConnectionUp)
    core.openflow.addListenerByName("PacketIn", _handle_PacketIn)
    core.openflow.addListenerByName("FlowRemoved", _handle_FlowRemoved)
    core.openflow.addListenerByName("FlowStatsReceived", _handle_flowstats_received)

# TODO
# change stat_collect_timeout for flows when timeout value changes- Done
# remove flow from active list when FlowRemoved msg is received
# Remove flow from schedule table when FlowRemoved message is received- Done

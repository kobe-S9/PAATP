import time

import heapq
import random

from collections import deque
from copy import deepcopy


MAX_RATE_BPS = 1e10
MIN_RATE_BPS = 1


USE_LATENCY_NOISE = False
MAX_LATENCY_NOISE = 1.1

BYTES_PER_PACKET = 1500


def calc_mean(lst):  # 计算平均值
    if len(lst) == 0:
        return 0.
    else:
        return sum(lst) * 1.0 / len(lst)


class Event(object):
    def __init__(self, t, obj, name, params={}):
        assert t is not None
        self.t = t #round(t, 6)
        self.obj = obj
        self.name = name
        self.params = params
    
    def __lt__(self, other):
        return (self.t, self.obj.TYPE, self.name, self.obj) < (other.t, other.obj.TYPE,  other.name, other.obj)

    def exec(self):
        #print('# event debug', self.t, self.obj, self.name, self.params)
        return getattr(self.obj, self.name)(**self.params)        


class Qdisc(object): #queueing discipline，排队规则
    _next_id = 1
    def _get_next_id():
        result = Qdisc._next_id
        Qdisc._next_id += 1
        return result

    def __init__(self, name=None):
        self.id = Qdisc._get_next_id()
        self.name = name or self.id
    
    def is_empty(self):
        return True

    def enq(self, pkt):
        pass

    def deq(self):
        return None
    
    def get_occupation(self):
        pass


class FIFO(object):
    def __init__(self):
        self.q = deque()
        self.total_pkt_cnt = 0
        self.total_pkt_size_in_bits = 0

    def clear(self):
        self.q.clear()
        self.total_pkt_cnt = 0
        self.total_pkt_size_in_bits = 0

    def enq(self, pkt):
        self.q.append(pkt)
        self.total_pkt_cnt += 1
        self.total_pkt_size_in_bits += pkt.size_in_bits
    
    def get_next_pkt(self):
        if self.total_pkt_cnt == 0:
            return None
        else:
            return self.q[0]

    def deq(self):
        if self.total_pkt_cnt == 0:
            return None
        else:
            pkt = self.q.popleft()
            self.total_pkt_cnt -= 1
            self.total_pkt_size_in_bits -= pkt.size_in_bits
            return pkt

    def __len__(self):
        return self.total_pkt_cnt
    
    def get_pkt_num(self):
        return self.total_pkt_cnt

    def get_occupation_in_bits(self):
        return self.total_pkt_size_in_bits


class SPQ(Qdisc):
    "Strict Priority Queue"
    def __init__(self, mq_sizes, ecn_threshold_fractors=None):
        self.priorities = sorted(mq_sizes)
        self.mq_sizes = mq_sizes
        self.ecn_threshold_fractors = ecn_threshold_fractors
        self.ecn_thresholds = {i: max(1, self.mq_sizes[i] * self.ecn_threshold_fractors[i]) for i in self.priorities}
        
        self.mq = {i: FIFO() for i in self.priorities} # to the dict of priority queues
            
    def enq(self, pkt):
        i = pkt.priority
        p = (self.mq[i].total_pkt_size_in_bits - self.mq_sizes[i] * 3 / 4) / (self.mq_sizes[i] / 4)
        if p >= random.random():
            pkt.drop(pkt.DROP_CAUSED_BY_CONGESTION)
            print("drop occur",pkt.path[pkt.hop_cnt-1].TYPE,pkt.path[pkt.hop_cnt-1].id,pkt.path[pkt.hop_cnt].TYPE,pkt.path[pkt.hop_cnt].id,"pkt.type",pkt.pkt_type,pkt.chunk_seq)
            return

        self.mq[i].enq(pkt)

        if self.mq[i].total_pkt_size_in_bits > self.ecn_thresholds[i]:
            if pkt.ecn == pkt.ECT:
                pkt.ecn = pkt.CE
    
    def deq(self):
        for i in self.priorities:
            if len(self.mq[i]) > 0:
                return self.mq[i].deq()
        return None

    def get_next_pkt(self):
        for i in self.priorities:
            if len(self.mq[i]) > 0:
                return self.mq[i].get_next_pkt()
        return None

    def ecn_config(self, q, ecn_threshold_factor):
        self.ecn_thresholds[q] = int(ecn_threshold_factor * self.mq_sizes[q])
        assert self.ecn_threshold_fractors[q] > 3 * 8e3

    def is_empty(self):
        for i in self.priorities:
            if len(self.mq[i]) > 0:
                return False
        return True
    
    def get_occupation_in_bits(self):#获取mq中包的总个数
        s = 0
        for q in self.mq.values():
            s += q.get_occupation_in_bits()
        return s

    def get_pkt_num(self):
        n = 0
        for q in self.mq.values():
            n += len(q)
        return n

    def copy(self):
        return deepcopy(self)


class DWRR(SPQ):#Deficit Weighted Round Robin，加权差额循环调度
    """
    https://en.wikipedia.org/wiki/Deficit_round_robin
    """
    Quantum = 1200 * 8
    def __init__(self, mq_sizes, ecn_threshold_fractors, weights=None):
        super().__init__(mq_sizes, ecn_threshold_fractors)
        if weights is None:
            self.weights = {i: 1 for i in self.priorities} #相等权重
        else:
            self.weights = weights    
        #max_weight = max(self.weights.values())
        min_weight = min(self.weights.values())
        self.DC = {i: 0 for i in self.priorities} #DeficitCounter
        self.Q = {i: self.Quantum * w / min_weight for i, w in self.weights.items()} #Quantum

    def deq(self):
        while 1:
            is_empty = True
            for i in self.priorities:
                if len(self.mq[i]) == 0:
                    self.DC[i] = 0
                    continue
                is_empty = False
                # all packets are with the equal size of 1
                pkt = self.mq[i].get_next_pkt()
                if self.DC[i] >= pkt.size_in_bits:
                    self.DC[i] -= pkt.size_in_bits
                    return self.mq[i].deq()
            assert not is_empty
            for i in self.priorities:
                self.DC[i] += self.Q[i]
        return None

    def get_next_pkt(self):
        while 1:
            is_empty = True
            for i in self.priorities:
                if len(self.mq[i]) == 0:
                    self.DC[i] = 0
                    continue
                is_empty = False
                # all packets are with the equal size of 1
                pkt = self.mq[i].get_next_pkt()
                if self.DC[i] >= pkt.size_in_bits:
                    return pkt
            assert not is_empty
            for i in self.priorities:
                self.DC[i] += self.Q[i]
        return None


class WFQ(SPQ):
    """
    https://en.wikipedia.org/wiki/Weighted_fair_queueing
    """
    def __init__(self, weights, mq_sizes, ecn_threshold_fractors=None):
        self.weights = weights
        super().__init__(mq_sizes, ecn_threshold_fractors)
        # TODO:  


class Link(object):
    TYPE = "LINK"
    _next_id = 1
    def _get_next_id():
        result = Link._next_id
        Link._next_id += 1
        return result

    def __init__(self, bandwidth, delay, qdisc, ecn_threshold_factor=1., loss_rate=0, name=None, **params):
        self.id = Link._get_next_id()
        self.name = name or self.id

        self.net = None
        self.bw_bps = float(bandwidth)
        self.ecn_threshold_factor = ecn_threshold_factor 

        self.qdisc = qdisc

        self.delay = delay
        self.lr = loss_rate
        #self.config_ecn(ecn_threshold_factor)
        self.unscheduled = True
        self.params = params 
    
    def __lt__(self, other):
        return self.id < other.id

    def get_cur_time(self):
        return self.net.get_cur_time()

    def get_next_sending_interval(self, pkt_size_in_bits):
        return pkt_size_in_bits / self.bw_bps
        
    def register_network(self, net):
        self.net = net

    def config_ecn(self, ecn_threshold_factor):
        pass
        #self.qdisc.config(ecn_threshold_factor=ecn_threshold_factor)

    def update_bw(self, new_bw_bps):
        self.bw_bps = new_bw_bps
        return []

    def pkt_enq(self, pkt):
        #current_time = self.get_cur_time()
        if random.random() < self.lr:
            pkt.drop(pkt.DROP_CAUSED_BY_LINK_ERROR)
        else:
            print('# link', self.id, 'received packet', pkt.chunk_seq, "flow_id",pkt.flow.id,"ping",pkt.ping_seq)
            self.qdisc.enq(pkt)
        
    def pkt_deq(self):
        #current_time = self.get_cur_time()
        return self.qdisc.deq()

    def on_pkt_sent(self):
       
        new_events = []

        cur_time = self.get_cur_time()
        pkt = self.pkt_deq()
        if pkt is not None:    
            # print('# link', self.id, 'sent packet', pkt.chunk_seq, "flow_id",pkt.flow.id)
            obj = pkt.get_next_hop()
            e = Event(
                cur_time + self.delay, 
                obj, 'on_pkt_received', 
                params=dict(pkt=pkt))

            new_events.append(e)
        else:
            assert self.qdisc.is_empty()
            
        if not self.qdisc.is_empty():#队列不空
            next_pkt = self.qdisc.get_next_pkt()
            t = self.get_next_sending_interval(next_pkt.size_in_bits)
            e = Event(cur_time + t, self, 'on_pkt_sent')
            new_events.append(e)
        else:
            self.unscheduled = True
        return new_events

    def on_pkt_received(self, pkt):
        pkt.hop()
        
        cur_time = self.get_cur_time()
        self.pkt_enq(pkt)  # 链路收到包，包进入队列
        new_events = []
        if self.unscheduled:#链路状态
            self.unscheduled = False
            next_pkt = self.qdisc.get_next_pkt()
            t = self.get_next_sending_interval(next_pkt.size_in_bits)
            e = Event(cur_time + t, self, 'on_pkt_sent')
            new_events.append(e)
        return new_events

        
class Network(object):
    def __init__(self, links=[], boxes=[], flows=[], others=[]):
        self.events = []
        self.cur_time = 0.0
        # links
        self.links = links
        self.named_links = {l.name: l for l in links}
        # boxes
        self.named_boxes = {b.name: b for b in boxes}
        # flows
        self.named_flows = {f.name: f for f in flows}
        # jobs
        self.others  = others

        #for atp
        self.ps_job_record = {}
        self.jobs_config = {}

        self.init_links()
        self.init_flows()
        self.init_boxes()
        self.init_others()

    def init_links(self):
        for l in self.named_links.values():
            l.register_network(self)
    
    def stop_links(self):
        pass

    def init_boxes(self):
        for b in self.named_boxes.values():
            b.register_network(self)
    
    def stop_boxes(self):
        for b in self.named_boxes.values():
            b.stop()

    def init_flows(self):
        for f in self.named_flows.values():
            f.register_network(self)
            self.add_events(f.start())

    def init_others(self):
        for o in self.others:
            o.register_network(self)
            self.add_events(o.start())
            
    def add_events(self, events):
        for e in events:
            heapq.heappush(self.events, e)

    def stop_flows(self):
        pass
    
    def get_cur_time(self):
        return self.cur_time

    def run(self, end_time=None):
        #end_time = self.cur_time + dur
        while end_time is None or self.cur_time < end_time:
            if len(self.events) == 0:
                break
            e = heapq.heappop(self.events)
            #if e.t < self.cur_time:
            #    print('xxshit', e.t - self.cur_time, e.t, self.cur_time)
            e.t = round(e.t, 9)
            assert e.t >= self.cur_time
            self.cur_time = e.t
            #print(self.cur_time, e.type, self.events)
            new_events = e.exec()
            for e in new_events:
                heapq.heappush(self.events, e)
        
        self.stop_boxes()



class Middlebox(object):
    TYPE = "Middlebox"

    _next_id = 1
    def _get_next_id():
        result = Middlebox._next_id
        Middlebox._next_id += 1
        return result

    def __init__(self, name=None):
        self.net = None
        self.id = Middlebox._get_next_id()
        self.name = name or self.id

    def register_network(self, net):
        self.net = net

    def on_pkt_received(self, pkt):
        print('# Middlebox received packet', pkt.chunk_seq, "flow_id",pkt.flow.id,"ping",pkt.ping_seq)
        
        pkt.hop()
        pkts = self.process(pkt)
        
        cur_time = self.get_cur_time()
        
        events = []

        for pkt in pkts:
            # print("#Middlebox send packet", pkt.chunk_seq, "flow_id",pkt.flow.id)
            obj = pkt.get_next_hop()
            e = Event(cur_time, obj, 'on_pkt_received', params=dict(pkt=pkt))
            events.append(e)

        return events
        
    def get_cur_time(self):
        return self.net.get_cur_time()

    def process(self, pkt):
        return pkt

    def stop(self):
        pass


class Packet(object):
    DROP_CAUSED_BY_LINK_ERROR = 1
    DROP_CAUSED_BY_CONGESTION = 2
    
    NOT_ECT = 0
    ECT = 1
    CE = 3
    def __init__(self, sent_time, priority, pkt_type=None, size_in_bits=0, flow=None, path=[], ecn=NOT_ECT, ece=None):
        self.sent_time = sent_time
        self.recv_time = None
        self.ack_time = None

        self.pkt_type = pkt_type
        self.size_in_bits = size_in_bits

        self.ecn = ecn
        self.ece = ece

        self.dropped = None
        self.enq_time = None
        self.deq_time = None

        self.priority = priority  # 优先级来自流的优先级
        self.flow = flow
        self.path = path

        self.hop_cnt = 0

        self.ack_seq = 0
        #for atp
        self.bitmap = {}
        self.resend = 0
        self.multicast = 0

        #for paatp 
        self.aecn = False
        self.Ssum = -1

        #for Muilti
        self.quantity_type = None
        self.min_chunk_seq = -1
        self.ping_seq = -1
        self.ack_cwd_seq = 0
    


    def hop(self):
        self.hop_cnt += 1

    def get_next_hop(self):
        if self.hop_cnt >= len(self.path):
            print('error', self.hop_cnt, self.path)
            raise ValueError
            return None
        else:
            return self.path[self.hop_cnt]

    def get_pkt_size(self):
        return self.size_in_bits
    
    def drop(self, reason):
        self.dropped = True
        self.flow.on_pkt_lost(reason, self)

    def __lt__(self, other):
        if self.flow.id == other.flow.id:  # 流相同则比较时间
            return self.sent_time < other.sent_time
        elif self.priority != other.priority:  # 比较优先级
            return self.priority < other.priority
        elif self.enq_time is not None and other.enq_time is not None:  # 比较入队时间
            return self.enq_time < other.enq_time
            #return random.choice([True, False])
        else:
            return self.sent_time < other.sent_time
    
    def __gt__(self, other):
        return not self.__lt__(other)
    

class Flow(object):
    UNSTARTED=1
    ONLINE=2
    COMPLETED=3
    EXPIRED=4

    TYPE = 'Flow'
    _next_id = 1
    def _get_next_id():
        result = Flow._next_id
        Flow._next_id += 1
        return result
    
    def __lt__(self, other):
        return True

    def __gt__(self, other):
        return True

    def __le__(self, other):
        return True

    def __ge__(self, other):
        return True

    def __init__(self, path, rpath, 
                 start_time=None, expired_time=None,
                 init_rate_bps=None, nic_rate_bps=None,
                 priority=2, stats=None, name=None, 
                 total_chunk_num=None, job_id=None,
                 **params):
        self.id = Flow._get_next_id()
        self.name = name or self.id
        self.job_id = job_id
        self.priority = priority

        self.start_time = start_time
        self.expired_time = expired_time
        if self.expired_time is not None and self.start_time is not None:
            assert self.expired_time > self.start_time

        self.init_rate_bps = init_rate_bps
        self.nic_rate_bps = nic_rate_bps
        self.rate_bps = init_rate_bps
        if self.rate_bps is None:
            self.rate_bps = self.nic_rate_bps
        self.rate_version = 0

        self.min_rate_bps = params.get('min_rate_bps', MIN_RATE_BPS)
        self.max_rate_bps = params.get('max_rate_bps', MAX_RATE_BPS)
        self.min_rate_delta = params.get('min_rate_delta', -1)
        self.max_rate_delta = params.get('max_rate_delta',  1)
        #self.check_completed = params.get('check_completed', lambda v: False)
        
        self.ecn_enabled = params.get('ecn_enabled', None)

        self.state = self.UNSTARTED

        self.stats = stats #[]
        self.lost_reason = {}
        self.total_lost = 0
        
        self.net = None

        self.opath = path
        self.orpath = rpath
        self.path = list(path)
        self.reversed_path = list(rpath)
        self.path.append(self)
        self.reversed_path.append(self)

        self.default_pkt_size_in_bits = params.get('pkt_size_in_bits', 8e3)

        self.params = params
        self.stop_time = None
        self.total_chunk_num = total_chunk_num
        self.completed_chunk_offset = -1
    
    def __del__(self):
        pass
   
    def clear(self):
        self.lost_reason.clear()

    def check_expired(self, cur_time=None):
        if self.expired_time is None:
            return False

        if self.state == self.ONLINE:
            if cur_time is None:
                cur_time = self.get_cur_time()
            if cur_time + 1e-9 > self.expired_time:
                print('# Flow', self.id, 'expired at', cur_time)
                self.state = self.EXPIRED
                self.stop_time = self.expired_time
        return self.state == self.EXPIRED

    def get_pkt_priority(self):
        return self.priority

    def on_pkt_received(self, pkt):
        pkt.hop()
        return []

    def get_sending_interval(self, pkt_size_in_bits=None):
        pkt_size_in_bits = pkt_size_in_bits or self.default_pkt_size_in_bits
        return pkt_size_in_bits / self.rate_bps #* (1+ 0.2 * random.random() - 0.1))
    
    def update_rate(self, version, new_rate_bps):
        if new_rate_bps < self.min_rate_bps:
            print('# zero rate error', new_rate_bps)
            new_rate_bps = self.min_rate_bps
        if self.rate_version == version:
            self.rate_bps = new_rate_bps
            # print('# Flow', self.id, 'new_rate (Mbps):', new_rate_bps / 1e6, 'version', version, 'at time', self.get_cur_time())
        return []

    def update_version(self, new_rate_version=None):
        if new_rate_version is None:
            self.rate_version += 1
        else:
            self.rate_version = new_rate_version
        return []
    
    def update_Q(self, Q):
        print('# Flow', self.id, 'new Q:', Q)
        if Q is None:
            return []
        
        self.Q = Q
        return []

    def get_waiting_interval(self, r=0.1):
        return r * self.get_sending_interval()

    def register_network(self, net):
        self.net = net

    def get_cur_time(self):
        return self.net.get_cur_time()

    def start(self):
        self.state = self.ONLINE
        return []

    def gen_stat(self):
        pass

    def should_record_stat(self):
        return self.stats is not None
        
    def append_stat(self, stat):
        if self.stats is None:
            return
        cur_time = self.get_cur_time()
        self.stats.append((cur_time, stat))
    
    def on_pkt_lost(self, reason, pkt=None):
        self.total_lost += 1
        self.lost_reason[reason] = 1 + self.lost_reason.get(reason, 0)
        self.last_drop_time = self.get_cur_time()
        self.last_drop_chunk_seq = pkt.chunk_seq


class Job(object):    
    TYPE = 'JOb'
    _next_id = 1
    def _get_next_id():
        result = Job._next_id
        Job._next_id += 1
        return result

    def __init__(self, name=None):
        self.id = Job._get_next_id()
        self.name = name or self.id
        self.net = None

    def register_network(self, net):
        self.net = net

    def get_cur_time(self):
        return self.net.get_cur_time()

    def start(self):
        return []
    

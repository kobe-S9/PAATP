from core import *

from collections import OrderedDict

class PingPongFlow(Flow):
    CC_STATE_SLOW_START = 1
    CC_STATE_CONGESTION_AVOIDANCE = 2
    CC_STATE_FAST_RECOVERY = 3

    PING_PKT_TYPE = 'PING'
    PONG_PKT_TYPE = 'PONG'

    PING_PKT_SIZE_IN_BITS = 8 * 1024
    PONG_PKT_SIZE_IN_BITS = 8 * 2

    DUPLICATED_PONG_THRESHOLD = 3

    def start(self):
        super().start()
        #self.job_id = self.params.get('job_id', None)

        #print('rate_Mps', self.rate_bps / 1e6)
        #self.sending_interval_var = 0.2
        self.cwnd = self.params.get('init_cwnd', 2)
        self.min_cwnd = self.params.get('min_cwnd', 1)
        self.max_cwnd = self.params.get('max_cwnd', None)
        
        self.min_ssthresh = self.params.get('min_ssthresh', 2)
        self.max_ssthresh = self.params.get('max_ssthresh', None)

        self.cc_state = self.CC_STATE_SLOW_START
        self.ssthresh = None

        self.chunk_seq = 0
        self.ping_seq = 0
        self.pong_seq = 0

        self.sent_ping = 0
        self.sent_pong = 0
        self.received_ping = 0
        self.received_pong = 0
        self.received_pong_for_throughput =0

        self.received_pong_from_last_timeout = 0
        
        self.ping_yet_unpong = {}
        self.out_of_order_cnts = {}

        self.resend_queue = OrderedDict()
    
        self.completed_chunks = {}
        #self.completed_chunk_offset = -1

        self.est_rtt = None
        self.dev_rtt = None
        self.rtt_alpha = self.params.get('rtt_alpha', 0.9)
        self.rtt_beta = self.params.get('rtt_beta', 0.9)

        self.ecn_enabled = False

        self.last_ssthresh_update_time = -1e3

        self.last_pong_received_time = None

        self.last_throughput_check_time = None

        self.pong_timeout_threshold = self.params.get('timeout_threshold', 1)
        
        events = []
        self.start_time = self.start_time or self.get_cur_time()
        if self.total_chunk_num != 0:
        
            e = Event(self.start_time + self.pong_timeout_threshold, self, "check_timeout")
            events.append(e)        
            e = Event(self.start_time, self, "on_ping_sent")
            events.append(e)
        else:
            self.state = self.COMPLETED

        return events

    def restart(self):
        events = []
        if self.state == self.ONLINE:
            return events
        else:
            cur_time = self.get_cur_time()
            self.state = self.ONLINE
            e = Event(cur_time + self.pong_timeout_threshold, self, "check_timeout")
            events.append(e)
            e = Event(cur_time, self, "on_ping_sent")
            events.append(e)
            return events
    
    def get_total(self):
        return self.total_chunk_num

    def check_completed(self):
        if self.total_chunk_num is None or self.get_completed_chunk_num() < self.total_chunk_num:
            return False
        self.state = self.COMPLETED
        if self.stop_time is None:
            self.completed_time = self.stop_time = self.get_cur_time()     
        return True
    
    def get_completed_chunk_num(self):
        return 1 + self.completed_chunk_offset # + len(self.completed_chunks)

    def check_timeout(self):
        cur_time = self.get_cur_time()
        new_events = []

        self.check_expired(cur_time)
        if self.state != self.ONLINE:
            return new_events

        # print('Flow', self.id, 'in timeout', self.ping_yet_unpong.keys(), self.resend_queue.keys(), self.total_chunk_num, self.completed_chunk_offset, self.received_pong_from_last_timeout, 'cwnd', self.cwnd)
        #raise UnicodeEncodeError
        if len(self.ping_yet_unpong) > 0 and self.received_pong_from_last_timeout == 0:      
            print('# Flow', self.id, 'is timeout at', cur_time, )
            print("last_pong_received_time",self.last_pong_received_time)          
            print("last_recived_chunk_seq",self.last_recived_chunk_seq)
            
            print("last_drop_time",self.last_drop_time)
            print("last_drop_chunk_seq",self.last_drop_chunk_seq)
            print("chunk_seq",self.chunk_seq)
            print("completed_chunk_offset",self.completed_chunk_offset)
            print("received_pong",self.received_pong)
            print("resend_queue",self.resend_queue)
            print("cwnd",self.cwnd)
            print("ping_yet_unpong",self.ping_yet_unpong)

            print("sssthresh",self.ssthresh)
            print("cc_state",self.cc_state)
            print("total_lost",self.total_lost)
            print("####################")
            print("self.net.link")
            for link in self.net.named_links.values():
                for priority, fifo_queue in link.qdisc.mq.items():
                    print(f"Priority {priority}:")
                    for pkt in fifo_queue.q:
                        print(f"Packet chunk_seq: {pkt.chunk_seq}")
            print("####################")
            print("self.net.boxes")
            for box in self.net.named_boxes.values():
               print(f"Box ID: {box.id}")
               for chunk_key, flow_set in box.mup_cache_meta.items():
                print(f"Chunk Key: {chunk_key}, Flow IDs: {flow_set}")
            print("####################")
            print("self.net.events")
            for event in self.net.events:
                if event.params:  # 只有 params 不为空时才打印
                    print(f"Event: t={event.t}, obj_type={event.obj.TYPE}, obj_id={event.obj.id}, name={event.name}")
                    for key, pkt in event.params.items():
                        print(f"  Pkt[{key}]: chunk_seq={pkt.chunk_seq}")

            self.cc_state = self.CC_STATE_SLOW_START
            self.update_ssthresh(self.cwnd * .5)
            self.update_cwnd(self.min_cwnd)
            self.last_ssthresh_update_time = cur_time
            # print('# old resend queue', self.resend_queue)
            for i in self.ping_yet_unpong:
                self.resend_queue[i] = 1
           
            # print('# new resend queue', self.resend_queue)
            self.ping_yet_unpong.clear()
            self.out_of_order_cnts.clear()

        self.received_pong_from_last_timeout = 0
        e = Event(cur_time + self.pong_timeout_threshold, self, "check_timeout")
        new_events.append(e)
        return new_events
    
    def send_ping(self, chunk_seq,resend_flag, delay=0.0):
        cur_time = self.get_cur_time()
        stat = dict(
            sent_chunk_seq_at_sender=chunk_seq,
            total_chunk_seq_at_sender=self.chunk_seq)
        #self.append_stat(stat)

        pkt = Packet(
            sent_time = cur_time, 
            priority = self.get_pkt_priority(), 
            pkt_type = self.PING_PKT_TYPE,
            size_in_bits = self.PING_PKT_SIZE_IN_BITS,
            flow = self, 
            path = self.path,
            ecn=Packet.ECT if self.ecn_enabled else Packet.NOT_ECT,
            )

        pkt.ping_seq = self.ping_seq
        self.ping_seq += 1
        pkt.chunk_seq = chunk_seq
        pkt.resend = resend_flag
        if Flow.TYPE == 'INA_PAATP' or Flow.TYPE == 'ATP':
           self.send_times[chunk_seq] = cur_time

        self.sent_ping += 1

        self.ping_yet_unpong[pkt.chunk_seq] = pkt
        self.out_of_order_cnts[pkt.chunk_seq] = 0

        obj = pkt.get_next_hop()
        print(f"flow {self.id} send ping {pkt.chunk_seq}")
        #delay = random.random() * 0.05 * self.get_sending_interval()
        e = Event(cur_time + delay, obj, 'on_pkt_received', params=dict(pkt=pkt))
        
        #print('# sent ping', pkt.ping_seq, 'chunk_seq', chunk_seq, 'cwnd', self.cwnd, 'received_pong', self.received_pong)
        return e, pkt

    def on_ping_sent(self):
        cur_time = self.get_cur_time()
        new_events = []

        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            return new_events
        if self.can_send_ping() and len(self.ping_yet_unpong) < self.cwnd:
            # ignore reordered packets
            if len(self.resend_queue) > 0:
                chunk_seq, _ = self.resend_queue.popitem(last=False)
                # print('resend detail', chunk_seq, self.resend_queue)
                e, _ = self.send_ping(chunk_seq,1)
                new_events.append(e)
            elif self.total_chunk_num is None or self.chunk_seq < self.total_chunk_num:
                chunk_seq = self.chunk_seq
                self.chunk_seq += 1

                e, _ = self.send_ping(chunk_seq,0)
                new_events.append(e)
            elif self.total_chunk_num is not None and self.chunk_seq >= self.total_chunk_num:
                pass
        if self.rate_bps is None:
            print(self.job_id, self.id, self.rate_bps)
        if self.rate_bps > 1e1:
            e = Event(cur_time + self.get_sending_interval(self.PING_PKT_SIZE_IN_BITS), self, "on_ping_sent")
            new_events.append(e)
        return new_events
    
    def can_send_ping(self):
        return True

    def on_pkt_received(self, pkt):
        new_events = super().on_pkt_received(pkt)

        if pkt.pkt_type == self.PING_PKT_TYPE:
      
            lst = self.on_ping_received(pkt)
        elif pkt.pkt_type == self.PONG_PKT_TYPE:
            lst = self.on_pong_received(pkt)

        return new_events + lst
    
    def on_ping_received(self, pkt):
        self.received_ping += 1
        new_events = []
        cur_time = self.get_cur_time()

        #if self.check_expired(cur_time):
        #    return new_events

        pkt.recv_time = cur_time
        
        pkt = self.change_ping_to_pong(pkt)
        
        obj = pkt.get_next_hop()
        # TODO: add receiver pacing here
        e = Event(cur_time, obj, 'on_pkt_received', params=dict(pkt=pkt))
        new_events.append(e)
            
        self.sent_pong += 1
        return new_events

    def change_ping_to_pong(self, pkt):
        """
        owd = pkt.recv_time - pkt.sent_time
        if self.stats is not None:
            stat = dict(owd=owd)
            self.stats.append((cur_time, stat))
        """
        pkt.pkt_type = self.PONG_PKT_TYPE
        pkt.size_in_bits = self.PONG_PKT_SIZE_IN_BITS

        pkt.ping_path = pkt.path
        pkt.ping_hop_cnt = pkt.hop_cnt

        pkt.path = self.reversed_path[-1 * pkt.ping_hop_cnt : ]
        pkt.hop_cnt = 0
        
        # deal with ecn flags
        if pkt.ecn == pkt.CE:
            pkt.ece = True
            pkt.ecn = pkt.ECT

        #pkt.priority = max(1, pkt.priority - 1)
        #if self.check_completed(self.received_chunks):
        #    return []
        return pkt
        
    def update_est_rtt(self, sample_rtt):
        if self.est_rtt is None:
            self.est_rtt = sample_rtt
        else:
            self.est_rtt = self.est_rtt * self.rtt_alpha + sample_rtt * (1 - self.rtt_alpha)
        
        sample_dev = sample_rtt - self.est_rtt
        if sample_dev < 0:
            sample_dev *= -1
        
        if self.dev_rtt is None:
            self.dev_rtt = sample_dev

        self.dev_rtt = self.dev_rtt * self.rtt_beta + sample_dev * (1 - self.rtt_beta)
        self.ack_timeout_interval = self.est_rtt + 4 * self.dev_rtt
        return self.est_rtt
    
    # TODO: on_pong_sent(self):
    def update_completed_chunk(self, chunk_seq):
        self.completed_chunks[chunk_seq] = 1

        if chunk_seq < self.completed_chunk_offset:
            return
        for i in range(self.completed_chunk_offset, chunk_seq + 1):
            if i in self.completed_chunks:
                del self.completed_chunks[i]
                self.completed_chunk_offset += 1
            else:
                break
        else:
            while self.completed_chunk_offset in self.completed_chunks:
                del self.completed_chunks[self.completed_chunk_offset]
                self.completed_chunk_offset += 1
        
    def on_pong_received(self, pkt):
        
        self.received_pong += 1
       

        if self.state != self.ONLINE:
            return []

        self.received_pong_from_last_timeout += 1
        #print('# get pong', pkt.ping_seq, pkt.chunk_seq, 'total', self.received_pong, self.completed_chunk_offset, len(self.completed_chunks), self.total_chunk_num, len(self.ping_yet_unpong))

        cur_time = self.get_cur_time()
        self.last_pong_received_time = cur_time
        sample_rtt = cur_time - pkt.sent_time
        self.update_est_rtt(sample_rtt)

        #print('# get pong chunk', cur_time, self.id, pkt.chunk_seq, self.completed_chunk_offset, len(self.completed_chunks), self.total_chunk_num)

        #self.acked_offsets.update(pkt.acked_offsets)

        pkt_chunk_seq = pkt.chunk_seq
        self.update_completed_chunk(pkt_chunk_seq)
        
        if pkt_chunk_seq in self.ping_yet_unpong:
            del self.ping_yet_unpong[pkt_chunk_seq]
            del self.out_of_order_cnts[pkt_chunk_seq]
        elif pkt_chunk_seq in self.resend_queue:
            del self.resend_queue[pkt_chunk_seq]
            
        #self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            return []
    
        # detect reorder
        to_resend = []
        for i, ipkt in self.ping_yet_unpong.items():
            if ipkt.ping_seq < pkt.ping_seq:
                self.out_of_order_cnts[i] += 1
                if self.out_of_order_cnts[i] >= self.DUPLICATED_PONG_THRESHOLD:
                    to_resend.append(i)
                    del self.out_of_order_cnts[i]
        for chunk_seq in to_resend:
            self.resend_queue[chunk_seq] = 1
            del self.ping_yet_unpong[chunk_seq]
        
        if len(to_resend) > 0:
            pass
            #print('#', cur_time, 'to_resend', len(to_resend), to_resend)
                    
        # TODO: xxx
        if self.cc_state == self.CC_STATE_CONGESTION_AVOIDANCE:
            if len(to_resend) > 0:
                if cur_time > self.last_ssthresh_update_time + self.est_rtt:
                    self.last_ssthresh_update_time = cur_time
                    cwnd = self.cwnd * 0.5
                    self.update_ssthresh(cwnd)
                    self.update_cwnd(cwnd)
                    #print('#', cur_time, 'ssthresh', self.ssthresh, 'to_resend', len(to_resend), to_resend)
            else:
                self.update_cwnd(self.cwnd + 1. / self.cwnd)

        elif self.cc_state == self.CC_STATE_SLOW_START:
            if len(to_resend) > 0:
                # switch to congestion avoidance
                self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE
                self.last_ssthresh_update_time = cur_time

                cwnd = self.cwnd * 0.5
                self.update_ssthresh(cwnd)
                self.update_cwnd(cwnd)
                #print('# Flow', self.id, 'ssthresh', self.ssthresh, '# from SLOW_START to CONGESTION_AVOIDANCE')
            else: 
                # stay in slow start
                self.update_cwnd(self.cwnd + 1)
        else:
            print('# error!')
            raise ValueError

        return []

    def update_cwnd(self, new_cwnd):
      
        if self.est_rtt is not None:
            allowed_cwnd = 1.0 * self.est_rtt * self.rate_bps / self.PING_PKT_SIZE_IN_BITS
            new_cwnd = min(allowed_cwnd, new_cwnd)
        else:
            pass
        #    allowed_cwnd = 

        new_cwnd = max(self.min_cwnd, new_cwnd)
        if self.max_cwnd is not None:
            new_cwnd = min(self.max_cwnd, new_cwnd)
        self.cwnd = new_cwnd
    
        self.gen_stat()
    
    def update_ssthresh(self, new_ssthresh):
        new_ssthresh = max(self.min_ssthresh, new_ssthresh)
        if self.max_ssthresh is not None:
            new_ssthresh = min(self.max_ssthresh, new_ssthresh)
        self.ssthresh = new_ssthresh        

    def gen_stat(self):
        if not self.should_record_stat():
            return
        stat = dict(
            ssthresh=self.ssthresh,
            cwnd=self.cwnd,
            rtt=self.est_rtt,
            rate=self.rate_bps,
            #ping_seq=self.ping_seq,
            chunk_seq=self.chunk_seq,
            ping_yet_unpong=len(self.ping_yet_unpong),
            #sent_ping=self.sent_ping,
            #sent_pong=self.sent_pong,
            #received_ping=self.received_ping,
            #received_pong=self.received_pong,
            cc=self.cc_state,
            lost=self.total_lost,
        )

        if self.TYPE == INA_PAATP.TYPE:
            stat["last_aack"] = self.last_recived_chunk_seq
            stat["last_cwd"] = self.last_cwd_received_seq
            stat["snd_nxt"] = self.snd_nxt
            stat["recieved_cwd"] = self.received_cwd

        for i, obj in enumerate(self.path):
            if obj.TYPE == 'LINK':
                stat['q_{0}'.format(i)] = obj.qdisc.get_occupation_in_bits()/self.PING_PKT_SIZE_IN_BITS
        for i, obj in enumerate(self.reversed_path):
            if obj.TYPE == 'LINK':
                stat['qr_{0}'.format(i)] = obj.qdisc.get_occupation_in_bits()/self.PONG_PKT_SIZE_IN_BITS
        if self.est_rtt is not None :
            mi_duration = self.est_rtt * 5
            cur_time = self.get_cur_time()
            if self.last_throughput_check_time is None:
                self.last_throughput_check_time = cur_time
                self.last_checked_completed_chunk_offset = self.completed_chunk_offset
            elif self.last_throughput_check_time + mi_duration < cur_time:
                stat['throughput_Mbps'] = (self.completed_chunk_offset - self.last_checked_completed_chunk_offset) / mi_duration  * self.PING_PKT_SIZE_IN_BITS / 1e6
                stat["received_for_throughput"] = self.received_pong_for_throughput/ mi_duration  * self.PING_PKT_SIZE_IN_BITS / 1e6
                self.received_pong_for_throughput = 0
                self.last_throughput_check_time = cur_time
                self.last_checked_completed_chunk_offset = self.completed_chunk_offset
  

        self.append_stat(stat)


class TCP_Reno(PingPongFlow):
    TYPE = 'TCP_Reno'
    PING_PKT_TYPE = 'DATA'
    PONG_PKT_TYPE = 'ACK'

    PING_PKT_SIZE_IN_BITS = 8 * 1024
    PONG_PKT_SIZE_IN_BITS = 8 * 2

    def start(self):
        self.recv_buf = {}
        self.chunk_seq_to_receive = 0
        self.last_ack_seq = -1
        self.last_ack_seq_duplication_cnt = 0
        return super().start()


    def change_ping_to_pong(self, pkt):
        pkt = super().change_ping_to_pong(pkt)
        
        stat = dict(
            received_chunk_seq=pkt.chunk_seq,
            chunk_seq_to_receive=self.chunk_seq_to_receive,
        )

        if self.chunk_seq_to_receive < pkt.chunk_seq:
            # pkt before this one might get lost
            self.recv_buf[pkt.chunk_seq] = pkt
            #raise ValueError
        elif self.chunk_seq_to_receive == pkt.chunk_seq:
            self.recv_buf[pkt.chunk_seq] = pkt
            while self.chunk_seq_to_receive in self.recv_buf:
                del self.recv_buf[self.chunk_seq_to_receive]
                self.chunk_seq_to_receive += 1
        elif self.chunk_seq_to_receive > pkt.chunk_seq:
            pass
        #print('recv buf', list(self.recv_buf))
        pkt.ack_seq = self.chunk_seq_to_receive
        

        stat['ack_seq_sent'] = pkt.ack_seq
        #self.append_stat(stat)

        return pkt

    def update_completed_chunk(self, ack_seq):
        if ack_seq <= self.completed_chunk_offset:
            return
        self.completed_chunk_offset = ack_seq - 1
        
    def on_pong_received(self, pkt):
        #print('what==', pkt.ack_seq, self.ping_yet_unpong.keys(), self.resend_queue.keys())
       
    
        self.received_pong += 1

        if self.state != self.ONLINE:
            print("return1")
            return []

        self.received_pong_from_last_timeout += 1
        #print('# get pong', pkt.ping_seq, pkt.chunk_seq, 'total', self.received_pong, self.completed_chunk_offset, len(self.completed_chunks), self.total_chunk_num, len(self.ping_yet_unpong))

        cur_time = self.get_cur_time()
        self.last_pong_received_time = cur_time
        sample_rtt = cur_time - pkt.sent_time
        self.update_est_rtt(sample_rtt)

        #print('Flow', self.id, cur_time, '# get pong chunk chunk_seq', pkt.chunk_seq, 'ack_seq', pkt.ack_seq, self.completed_chunk_offset, len(self.completed_chunks), self.total_chunk_num)

        #self.acked_offsets.update(pkt.acked_offsets)
        self.update_completed_chunk(pkt.ack_seq)
        
        if self.last_ack_seq == pkt.ack_seq:
            self.last_ack_seq_duplication_cnt += 1
            #raise ValueError
        elif self.last_ack_seq > pkt.ack_seq:
            pass
        elif self.last_ack_seq < pkt.ack_seq:
            self.last_ack_seq_duplication_cnt = 0
            while self.last_ack_seq < pkt.ack_seq:
                if self.last_ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[self.last_ack_seq]
                elif self.last_ack_seq in self.resend_queue:
                    del self.resend_queue[self.last_ack_seq]
                self.last_ack_seq += 1
        """
        for i in self.ping_yet_unpong:
            if i < pkt.ack_seq:
                print('xxxx', i, pkt.ack_seq, self.last_ack_seq)
                raise ValueError
        # no use
        if len(self.resend_queue) != 0:
            print('recend_queue', self.resend_queue)
            #raise ValueError
        """

        # self.out_of_order_cnts.clear()
            
        
        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            print("return2 ,self.state",self.state)
            return []

        # detect reorder
        to_resend = []
        if self.last_ack_seq_duplication_cnt >= self.DUPLICATED_PONG_THRESHOLD:
            if pkt.ack_seq not in self.resend_queue:
                to_resend.append(pkt.ack_seq)
                self.last_ack_seq_duplication_cnt = 0
                self.resend_queue[pkt.ack_seq] = 1
                if pkt.ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[pkt.ack_seq]
        # print('sender gets ack seq', pkt.ack_seq, 'inflight', self.ping_yet_unpong.keys(), self.resend_queue.keys())
        new_events = []            
        # TODO: xxx
        if self.cc_state == self.CC_STATE_CONGESTION_AVOIDANCE:
            if len(to_resend) > 0:
                if cur_time > self.last_ssthresh_update_time + self.est_rtt:
                    self.last_ssthresh_update_time = cur_time
                    cwnd = self.cwnd * 0.5
                    self.update_ssthresh(cwnd)
                    self.update_cwnd(cwnd)
                    self.cc_state = self.CC_STATE_FAST_RECOVERY                    
                    # fast recovery
                    """
                    for chunk_seq in to_resend:
                        e, _ = self.send_ping(chunk_seq)
                        new_events.append(e)
                    print('# FASTRECOVERY', cur_time, 'ssthresh', self.ssthresh, 'to_resend', len(to_resend), to_resend)
                    """
            else:
                self.update_cwnd(self.cwnd + 1. / self.cwnd)

        elif self.cc_state == self.CC_STATE_FAST_RECOVERY:#快速恢复后状态改为拥塞避免
            if self.last_ack_seq_duplication_cnt == 0:
                # get a new ack
                #self.cwnd = self.ssthresh
                self.update_cwnd(self.ssthresh)#拥塞窗口改为门限
                #print('# set cwnd as', self.cwnd, self.ssthresh)
                self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE#拥塞避免
            else: #相等 # duplicated ack
                #self.cwnd += 1
                self.update_cwnd(self.cwnd + 5)

        elif self.cc_state == self.CC_STATE_SLOW_START:
            if len(to_resend) > 0:
                # switch to congestion avoidance
                self.cc_state = self.CC_STATE_FAST_RECOVERY
                self.last_ssthresh_update_time = cur_time

                cwnd = self.cwnd * 0.5
                self.update_ssthresh(cwnd)
                self.update_cwnd(cwnd)
                """
                for chunk_seq in to_resend:
                    e, _ = self.send_ping(chunk_seq)
                    new_events.append(e)
                print('# FASTRECOVERY', cur_time, 'ssthresh', self.ssthresh, 'to_resend', len(to_resend), to_resend)
                """
            else: 
                # stay in slow start
                
                self.update_cwnd(self.cwnd + 5)
               
                if self.ssthresh is not None and self.ssthresh < self.cwnd:
                    self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE
        else:
            print('# error!')
            raise ValueError
      
        return new_events


class DCTCP(TCP_Reno):
    TYPE = 'DCTCP'

    def start(self):
        events = super().start()
        self.dctcp_a = 0.
        self.dctcp_g = self.params.get('dctcp_g', 1. / 16)
        self.dctcp_last_time = None
        self.dctcp_pkt_cnt = 0
        self.dctcp_ecn_cnt = 0
        self.ecn_enabled = True
        return events

    def on_pong_received(self, pkt):
        
        cur_time = self.get_cur_time()
        self.dctcp_pkt_cnt += 1.
        if pkt.ece:
            self.dctcp_ecn_cnt += 1.
            if self.cc_state == self.CC_STATE_SLOW_START:
                self.update_ssthresh(self.cwnd * 0.5)
                #self.cwnd = max(1., self.cwnd * 0.5)
                self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE
            #print('ece', cur_time, self.cwnd)

        
        new_events = super().on_pong_received(pkt)
  
        if self.dctcp_last_time is None:
            self.dctcp_last_time = cur_time
        #http://blog.chinaunix.net/uid-1728743-id-4945682.html dctcp简介
        if self.dctcp_last_time + self.est_rtt < cur_time:
            dctcp_F = self.dctcp_ecn_cnt / self.dctcp_pkt_cnt
            self.dctcp_a = self.dctcp_a * (1 - self.dctcp_g) + self.dctcp_g * dctcp_F
            cwnd = self.cwnd * (1 - self.dctcp_a *.5)
            self.update_cwnd(cwnd)
            self.dctcp_pkt_cnt = 0
            self.dctcp_ecn_cnt = 0
            self.dctcp_last_time = cur_time
            
            stat = dict(dctcp_F=dctcp_F, dctcp_a=self.dctcp_a)
            self.append_stat(stat)
        

        return new_events


class DCTCPi(DCTCP):
    TYPE = 'DCTCPi'
    """
    improved for rtt-fairness
    https://people.csail.mit.edu/alizadeh/papers/dctcp_analysis-sigmetrics11.pdf
    """
    def on_pong_received(self, pkt):
        cur_cwnd = self.cwnd
        new_events = super().on_pong_received(pkt)
        #self.cwnd = cur_cwnd
        if pkt.ece:
            cwnd = cur_cwnd - self.dctcp_a / 2
            self.update_cwnd(cwnd)
        #print('DCTCPi flow', self.id, 'received ack_seq', pkt.ack_seq, 'cwnd', self.cwnd, self.get_cur_time())
        return new_events

class ATP(TCP_Reno):
    TYPE = 'ATP'

    PING_PKT_SIZE_IN_BITS = 8 * 300
    PONG_PKT_SIZE_IN_BITS = 8 * 300

    def start(self):
        events = super().start()
        self.send_times = {}
        self.last_drop_time = 0
        self.last_drop_chunk_seq = -1
        self.last_recived_chunk_seq = -1
        self.ecn_enabled = True
        return events

    def send_ping(self, chunk_seq, delay=0.0):
        events = super().send_ping(chunk_seq, delay)
        cur_time = self.get_cur_time()
        self.send_times[chunk_seq] = cur_time
        return events
    
    
    def on_ping_received(self, pkt):
        
        print(f"flow {self.id} received ping {pkt.chunk_seq}")
        self.received_ping += 1
        new_events = []
        cur_time = self.get_cur_time()
        pkt.recv_time = cur_time
        #if self.check_expired(cur_time):
        #    return new_events

        if(pkt.resend):
            if(len(self.net.ps_job_record[pkt.flow.job_id])==self.net.jobs_config[pkt.flow.job_id]['flownum']):
                #a worker lost ack
                print("flow",self.id," ack ",pkt.chunk_seq,"drop")

                if pkt.ecn == pkt.CE:
                    pkt.ece = True
                    pkt.ecn = pkt.ECT
                pkt.path=self.reversed_path
                pkt.priority=self.get_pkt_priority()
                pkt.pkt_type=self.PONG_PKT_TYPE
                pkt.hop_cnt = 0

                obj = pkt.get_next_hop()
                e = Event(cur_time, obj, 'on_pkt_received', params=dict(pkt=pkt))
                new_events.append(e)
                self.sent_pong += 1  
            else:
                #worker's gradient was sent to ps and aggrregation at ps
                print("flow",self.id," resend ",pkt.chunk_seq,"arrived ps")
                self.net.ps_job_record[pkt.flow.job_id][pkt.flow.id] = 1
                if self.chunk_seq_to_receive < pkt.chunk_seq:
                    self.recv_buf[pkt.chunk_seq] = pkt
                elif self.chunk_seq_to_receive == pkt.chunk_seq:
                    self.recv_buf[pkt.chunk_seq] = pkt
                    while self.chunk_seq_to_receive in self.recv_buf:
                        del self.recv_buf[self.chunk_seq_to_receive]
                        self.chunk_seq_to_receive += 1
                elif self.chunk_seq_to_receive > pkt.chunk_seq:
                    pass
            
                pkt.ack_seq = self.chunk_seq_to_receive

                if(len(self.net.ps_job_record[pkt.flow.job_id])==self.net.jobs_config[pkt.flow.job_id]['flownum']):
                    print("ps complete aggregation")
                    pkt.multicast = 1
                    if pkt.ecn == pkt.CE:
                        pkt.ece = True
                        pkt.ecn = pkt.ECT
                    pkt. path=self.reversed_path
                    pkt.priority=self.get_pkt_priority()
                    pkt.pkt_type=self.PONG_PKT_TYPE
                    pkt.hop_cnt = 0

                    obj = pkt.get_next_hop()
                    e = Event(cur_time, obj, 'on_pkt_received', params=dict(pkt=pkt))
                    new_events.append(e)
                    
                    self.sent_pong += 1
                else:
                    pass
        else:
            #sw sent a pkt to ps
            print("flow",self.id," pkt ",pkt.chunk_seq,"arrived ps")
            print(" self.net.ps_job_record", self.net.ps_job_record)
            print("pkt.bitmap",pkt.bitmap)
            self.net.ps_job_record[pkt.flow.job_id].update(pkt.bitmap)
            print(" self.net.ps_job_record", self.net.ps_job_record)
            for flow in self.net.named_flows.values():
                if flow.job_id == pkt.flow.job_id:
                    if flow.chunk_seq_to_receive < pkt.chunk_seq:
                        flow.recv_buf[pkt.chunk_seq] = pkt
                    elif flow.chunk_seq_to_receive == pkt.chunk_seq:
                        flow.recv_buf[pkt.chunk_seq] = pkt
                        while flow.chunk_seq_to_receive in flow.recv_buf:
                            del flow.recv_buf[flow.chunk_seq_to_receive]
                            flow.chunk_seq_to_receive += 1
                    elif flow.chunk_seq_to_receive > pkt.chunk_seq:
                        pass
                
            pkt.ack_seq = self.chunk_seq_to_receive
            print("len(self.net.ps_job_record[pkt.flow.job_id])",len(self.net.ps_job_record[pkt.flow.job_id]))
            print("self.net.jobs_config[pkt.flow.job_id]['flownum']",self.net.jobs_config[pkt.flow.job_id]['flownum'])
            if(len(self.net.ps_job_record[pkt.flow.job_id])==self.net.jobs_config[pkt.flow.job_id]['flownum']):
                print("sw complete aggregation")
                pkt.multicast = 1
                if pkt.ecn == pkt.CE:
                    pkt.ece = True
                    pkt.ecn = pkt.ECT
                pkt.path=self.reversed_path
                pkt.priority=self.get_pkt_priority()
                pkt.pkt_type=self.PONG_PKT_TYPE
                pkt.hop_cnt = 0

                obj = pkt.get_next_hop()
                e = Event(cur_time, obj, 'on_pkt_received', params=dict(pkt=pkt))
                new_events.append(e)
       
                self.sent_pong += 1
        return new_events
        
    def on_pong_received(self, pkt):
        print(f"flow {self.id} received pong {pkt.chunk_seq}")
        self.received_pong += 1
        self.received_pong_for_throughput += 1
        if self.state != self.ONLINE:
            print("return1")
            return []

        self.received_pong_from_last_timeout += 1
 
        cur_time = self.get_cur_time()
        self.last_pong_received_time = cur_time
        sample_rtt = cur_time - pkt.sent_time
        self.update_est_rtt(sample_rtt)

        self.update_completed_chunk(pkt.ack_seq)
        
        if self.last_ack_seq == pkt.ack_seq:
            self.last_ack_seq_duplication_cnt += 1
            #raise ValueError
        elif self.last_ack_seq > pkt.ack_seq:
            pass
        elif self.last_ack_seq < pkt.ack_seq:
            self.last_ack_seq_duplication_cnt = 0
            while self.last_ack_seq < pkt.ack_seq:
                if self.last_ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[self.last_ack_seq]
                elif self.last_ack_seq in self.resend_queue:
                    del self.resend_queue[self.last_ack_seq]
                self.last_ack_seq += 1
            
        
        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            print("return2 ,self.state",self.state)
            return []

        # detect reorder
        to_resend = []
        if self.last_ack_seq_duplication_cnt >= self.DUPLICATED_PONG_THRESHOLD:
            if pkt.ack_seq not in self.resend_queue:
                to_resend.append(pkt.ack_seq)
                self.last_ack_seq_duplication_cnt = 0
                self.resend_queue[pkt.ack_seq] = 1
                if pkt.ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[pkt.ack_seq]
       
        new_events = []            

        if self.cc_state == self.CC_STATE_CONGESTION_AVOIDANCE:
            if len(to_resend) > 0:
                if cur_time > self.last_ssthresh_update_time + self.est_rtt:
                    self.last_ssthresh_update_time = cur_time
                    cwnd = self.cwnd * 0.5
                    self.update_ssthresh(cwnd)
                    self.update_cwnd(cwnd + 3)
                    self.cc_state = self.CC_STATE_FAST_RECOVERY                    
                    # fast recovery
            elif pkt.ece:
                if cur_time > self.last_ssthresh_update_time + self.est_rtt:
                    self.last_ssthresh_update_time = cur_time
                    cwnd = self.cwnd * 0.5
                    self.update_ssthresh(cwnd)
                    self.update_cwnd(cwnd)
                    self.cc_state = self.CC_STATE_FAST_RECOVERY   
            else:
                self.update_cwnd(self.cwnd + 5. / self.cwnd)

        elif self.cc_state == self.CC_STATE_FAST_RECOVERY:#快速恢复后状态改为拥塞避免
            if self.last_ack_seq_duplication_cnt == 0:
                # get a new ack
                #self.cwnd = self.ssthresh
                self.update_cwnd(self.ssthresh)#拥塞窗口改为门限
                #print('# set cwnd as', self.cwnd, self.ssthresh)
                self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE#拥塞避免
            else: #相等 # duplicated ack
                #self.cwnd += 1
                self.update_cwnd(self.cwnd + 1)

        elif self.cc_state == self.CC_STATE_SLOW_START:
            if len(to_resend) > 0:
                # switch to congestion avoidance
                self.cc_state = self.CC_STATE_FAST_RECOVERY
                self.last_ssthresh_update_time = cur_time

                cwnd = self.cwnd * 0.5
                self.update_ssthresh(cwnd)
                self.update_cwnd(cwnd + 3)
            elif pkt.ece:
                # switch to congestion avoidance
                self.cc_state = self.CC_STATE_FAST_RECOVERY
                self.last_ssthresh_update_time = cur_time
                cwnd = self.cwnd * 0.5
                self.update_ssthresh(cwnd)
                self.update_cwnd(cwnd)
            else: 
                # stay in slow start
                
                self.update_cwnd(self.cwnd + 5)
               
                if self.ssthresh is not None and self.ssthresh <= self.cwnd:
                    self.cc_state = self.CC_STATE_CONGESTION_AVOIDANCE
        else:
            print('# error!')
            raise ValueError

        self.last_recived_chunk_seq = pkt.chunk_seq



        return new_events



    def on_pkt_received(self, pkt):
        if pkt.pkt_type == self.PONG_PKT_TYPE:
            lst = self.on_pong_received(pkt)
        elif pkt.pkt_type == self.PING_PKT_TYPE:
            lst = self.on_ping_received(pkt)
        return lst

    def update_completed_chunk(self, ack_seq):
        if ack_seq <= self.completed_chunk_offset:
            return
        self.completed_chunk_offset = ack_seq - 1
        if ack_seq > self.last_drop_chunk_seq:
            self.last_drop_time = self.get_cur_time()
            self.last_drop_chunk_seq = ack_seq

    def update_cwnd(self, new_cwnd):
        # if self.last_drop_time + self.est_rtt < self.get_cur_time():
        #     new_cwnd = new_cwnd * 2
        super().update_cwnd(new_cwnd)


class INA_PAATP(ATP):
    TYPE = 'INA_PAATP'

    PING_PKT_TYPE = 'PING'
    PONG_PKT_TYPE = 'PONG'
    CWD_PKT_TYPE = 'CWD'


    PING_PKT_SIZE_IN_BITS = 8*300#300bytes
    PONG_PKT_SIZE_IN_BITS = 8*300 #300bytes
    CWD_PKT_SIZE_IN_BITS = 8*62 #62bytes
 
   

    def start(self):
        events = super().start()
        self.send_times = {}
        self.last_pong_received_time = 0
        self.last_recived_chunk_seq = -1
        self.last_drop_time = 0
        self.last_drop_chunk_seq = -1
        
        self.ecn_enabled = True

        #paatp
        self.last_cwd_received_seq = -1
        self.last_recived_chunk_seq = -1
        self.received_cwd = 0

        self.cwnd = self.params.get('init_cwnd', 5)
        self.awd = self.params.get('awd', 15)
        self.snd_nxt = 0

        self.factors = 0.125#both f and w
        self.cwd_alpha = 0 #拥塞程度
        self.p = 0

        self.cwd_pkt_cnt = 0
        self.ack_pkt_cnt = 0
        self.cwd_ecn_cnt = 0
        self.aecn_pkt_cnt = 0
        self.last_cwd_alpha_update_time = 0
        self.last_awd_beta_update_time = 0
        self.last_cwnd_update_time  = 0
        self.awd_beta  = 0

        return events
    
    def on_ping_sent(self):
        cur_time = self.get_cur_time()
        new_events = []

        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            return new_events
        
        if self.can_send_ping():
            # ignore reordered packets
            if len(self.resend_queue) > 0:
                chunk_seq, _ = self.resend_queue.popitem(last=False)
                # print('resend detail', chunk_seq, self.resend_queue)
                e, _ = self.send_ping(chunk_seq,1)
                new_events.append(e)
            elif self.total_chunk_num is None or self.chunk_seq < self.total_chunk_num:
                if len(self.ping_yet_unpong) < self.awd and self.chunk_seq <= self.snd_nxt:
                    chunk_seq = self.chunk_seq
                    self.chunk_seq += 1

                    e, _ = self.send_ping(chunk_seq,0)
                    new_events.append(e)
            elif self.total_chunk_num is not None and self.chunk_seq >= self.total_chunk_num:
                pass
        if self.rate_bps is None:
            print(self.job_id, self.id, self.rate_bps)
        if self.rate_bps > 1e1:
            e = Event(cur_time + self.get_sending_interval(self.PING_PKT_SIZE_IN_BITS), self, "on_ping_sent")
            new_events.append(e)
        return new_events
    
    def on_pkt_received(self, pkt):
        if pkt.pkt_type == self.PONG_PKT_TYPE :
            lst = self.on_pong_received(pkt)
        elif pkt.pkt_type == self.CWD_PKT_TYPE:
            lst = self.on_cwd_received(pkt)
        elif pkt.pkt_type == self.PING_PKT_TYPE:
            lst = self.on_ping_received(pkt)

        return  lst

    def on_pong_received(self, pkt):
        cur_time = self.get_cur_time()

        self.last_pong_received_time = cur_time
        self.last_recived_chunk_seq = pkt.chunk_seq
        self.snd_nxt = min(self.last_recived_chunk_seq+self.awd,self.last_cwd_received_seq+self.cwnd)
        # print("on_pong_recieved stats",self.stats)
        print(f"flow {self.id} received pong {pkt.chunk_seq}")
        self.received_pong += 1
        self.received_pong_for_throughput += 1
        if self.state != self.ONLINE:
            print("return1")
            return []

        self.received_pong_from_last_timeout += 1
        self.last_pong_received_time = cur_time
        sample_rtt = cur_time - pkt.sent_time
        
        self.update_est_rtt(sample_rtt)
        self.update_completed_chunk(pkt.ack_seq)
        
        if self.last_ack_seq == pkt.ack_seq:
            self.last_ack_seq_duplication_cnt += 1
            #raise ValueError
        elif self.last_ack_seq > pkt.ack_seq:
            pass
        elif self.last_ack_seq < pkt.ack_seq:
            self.last_ack_seq_duplication_cnt = 0
            while self.last_ack_seq < pkt.ack_seq:
                if self.last_ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[self.last_ack_seq]
                elif self.last_ack_seq in self.resend_queue:
                    del self.resend_queue[self.last_ack_seq]
                self.last_ack_seq += 1
            
        
        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            print("return2 ,self.state",self.state)
            return []

        # detect reorder
        to_resend = []
        if self.last_ack_seq_duplication_cnt >= 1:
            if pkt.ack_seq not in self.resend_queue:
                to_resend.append(pkt.ack_seq)
                self.last_ack_seq_duplication_cnt = 0
                self.resend_queue[pkt.ack_seq] = 1
                if pkt.ack_seq in self.ping_yet_unpong:
                    del self.ping_yet_unpong[pkt.ack_seq]
       
        new_events = []            

        
       
        self.ack_pkt_cnt += 1
        if pkt.aecn:
            self.aecn_pkt_cnt += 1.
        else:
            self.awd += 1
            self.gen_stat()
        if cur_time > self.last_awd_beta_update_time + self.est_rtt:
            self.last_awd_beta_update_time = cur_time
            awd_G= self.aecn_pkt_cnt / self.ack_pkt_cnt
            self.awd_beta = (1 - self.factors) * self.awd_beta + self.factors * awd_G
            
            self.aecn_pkt_cnt = 0 
            self.ack_pkt_cnt = 0

            stat = dict(awd_G = awd_G, awd_beta  = self.awd_beta)
            self.append_stat(stat)
        
        if pkt.aecn and cur_time > self.last_awd_update_time + self.est_rtt:
            self.last_awd_update_time = cur_time
            self.awd = self.awd * (1 - self.awd_beta /.5)
            
            stat = dict(awd=self.awd)
            self.append_stat(stat)

        
        self.gen_stat()
        return new_events
    
    def on_cwd_received(self,pkt):
        print(f"flow {self.id} received cwd {pkt.chunk_seq}")
        self.received_cwd += 1
        cur_time = self.get_cur_time()
        new_events = []
        #cwd
        self.last_cwd_received_seq = pkt.chunk_seq
        self.snd_nxt = min(self.last_cwd_received_seq+self.cwnd,self.last_recived_chunk_seq+ self.awd)
        
        #awd
        self.Ssum = pkt.Ssum
        self.Ssavg = self.Ssum / self.net.jobs_config[self.job_id]["flownum"]
        if(self.Ssavg - self.last_recived_chunk_seq==0):
            print("edge:meta,Ssum,last_aggragated_num")
            print(next(iter(self.net.named_boxes.values())).mup_cache_meta)
            print(next(iter(self.net.named_boxes.values())).mup_cache.Ssum)
            print(next(iter(self.net.named_boxes.values())).mup_cache.last_gradient_seq)

            flows_for_job_i = [f for f in self.net.named_flows.values() if (f.job_id == pkt.flow.job_id and f.TYPE == pkt.flow.TYPE)]
            for flow in flows_for_job_i:
                print("last_chunk_seq,cwd,drop")
                print(self.last_recived_chunk_seq,self.last_cwd_received_seq,self.last_drop_chunk_seq)
            
        self.Pavg = self.Ssavg - self.last_recived_chunk_seq
        self.pl = self.last_cwd_received_seq - self.last_recived_chunk_seq 
        self.p = self.pl / self.Pavg
        self.pmax = self.awd / self.Pavg
   
        
        if pkt.ecn==Packet.CE:
            pkt.ece = True 

        self.cwd_pkt_cnt += 1
        if pkt.ece:
            self.cwd_ecn_cnt += 1.
        else:
            self.cwnd += 1
            self.update_cwnd(self.cwnd)

        if self.est_rtt != None :
            if cur_time > self.last_cwd_alpha_update_time + self.est_rtt:
                self.last_cwd_alpha_update_time = cur_time
                cwd_F = self.cwd_ecn_cnt / self.cwd_pkt_cnt
                self.cwd_alpha = (1 - self.factors) * self.cwd_alpha + self.factors * cwd_F
                
                self.cwd_ecn_cnt = 0 
                self.cwd_pkt_cnt = 0

                stat = dict(cwd_F = cwd_F, cwd_alpha  = self.cwd_alpha)
                self.append_stat(stat)
            
            if pkt.ece and cur_time > self.last_cwnd_update_time + self.est_rtt:
                self.last_cwnd_update_time = cur_time
                cwnd_D =  self.cwd_alpha ** (1 - self.p / self.pmax)
                cwnd = self.cwnd * (1-cwnd_D / .5)
                self.update_cwnd(cwnd)
                
                stat = dict(cwnd_D=cwnd_D)
                self.append_stat(stat)
        #     else: 
        #         self.cwnd += 1
        #         self.update_cwnd(self.cwnd)


        self.gen_stat()
        return new_events

class Muilt(INA_PAATP):
    TYPE = 'Muilt'

    PING_PKT_SIZE_IN_BITS = 8 * 112
    PONG_PKT_SIZE_IN_BITS = 8 * 300
    CWD_PKT_SIZE_IN_BITS = 8 * 62

    type_a = 'A'
    type_b = 'B'
    type_c = 'C'
    type_d = 'D'

    type_ab = 'AB'
    type_abc = 'ABC'
    type_abcd = 'ABCD'
   
    def start(self):
        Events  = super().start()

        
        self.Q = self.params.get('Q', 1)
        self.ping_yet_uncwd = {}

        self.A = 0
        self.B = 0
        self.C = 0
        self.D = 0

        return Events
    
    def on_ping_sent(self):
        cur_time = self.get_cur_time()
        new_events = []

        self.check_expired(cur_time)
        self.check_completed()
        if self.state != self.ONLINE:
            return new_events
        
        if self.can_send_ping():
            # ignore reordered packets
            if len(self.resend_queue) > 0:
                chunk_seq, _ = self.resend_queue.popitem(last=False)
                # print('resend detail', chunk_seq, self.resend_queue)
                e, _ = self.send_ping(chunk_seq,1)
                new_events.append(e)
            elif self.total_chunk_num is None or self.chunk_seq < self.total_chunk_num:
                if self.Q == 1:
                    if self.A == 0:
                        if len(self.ping_yet_unpong) < self.awd and len(self.ping_yet_uncwd) < self.cwnd:
                            chunk_seq = self.chunk_seq
                            ping_seq = self.ping_seq
                            self.chunk_seq += 1
                            self.ping_seq  += 1
                            quantify_type =  self.type_a

                            e, _ = self.send_ping(chunk_seq,ping_seq,quantify_type,0)
                            new_events.append(e)
                    elif self.A == self.type_ab:
                elif self.Q == 2: 
                    if len(self.ping_yet_uncwd) < self.cwnd:
                        if self.A == 0:
                            if len(self.ping_yet_unpong) < self.awd:
                                chunk_seq = self.chunk_seq
                                ping_seq = self.ping_seq
                                self.chunk_seq += 1
                                self.ping_seq  += 1
                                quantify_type =  self.type_ab

                                e, _ = self.send_ping(chunk_seq,ping_seq,quantify_type,0)
                                new_events.append(e)
          

     
            elif self.total_chunk_num is not None and self.chunk_seq >= self.total_chunk_num:
                pass
        if self.rate_bps is None:
            print(self.job_id, self.id, self.rate_bps)
        if self.rate_bps > 1e1:
            e = Event(cur_time + self.get_sending_interval(self.PING_PKT_SIZE_IN_BITS), self, "on_ping_sent")
            new_events.append(e)
        return new_events
    
    

class MDPCache(object):
    def __init__(self, max_total_cache_size, max_per_job_cache_size=None):
        self.max_total_cache_size = max_total_cache_size
        self.max_per_job_cache_size = max_per_job_cache_size
        
        self.data = {}
        self.data_flat = OrderedDict()
        self.max_chunk_seq = {}
        self.last_gradient_seq = {}
        self.Ssum = {}
    
    def update_gradient_seq(self, job_id,flow_id,chunk_id):

        if job_id not in self.last_gradient_seq:
            self.last_gradient_seq[job_id] = {}
   
        if flow_id not in self.last_gradient_seq[job_id]:
            self.last_gradient_seq[job_id][flow_id] = chunk_id
       
        else:
            old_id = self.last_gradient_seq[job_id][flow_id]
            self.last_gradient_seq[job_id][flow_id] = max(old_id, chunk_id)

        self.Ssum[job_id] = sum(self.last_gradient_seq[job_id].values())

        return self.Ssum[job_id]
    

    def move_to_end(self, job_id, chunk_id,flow_id):
        self.data[job_id].move_to_end(chunk_id)
        self.data_flat.move_to_end((job_id, chunk_id,flow_id,))

    def has_cache(self, job_id, chunk_id,flow_id):

        return (job_id, chunk_id,flow_id) in self.data_flat
        

    def update_cache(self, job_id, chunk_id,flow_id, cur_time):

        self.data_flat[(job_id, chunk_id,flow_id)] = cur_time
        self.data.setdefault(job_id, {}).setdefault(chunk_id, {}).update({flow_id: cur_time})
        self.max_chunk_seq[job_id] = max(chunk_id, self.max_chunk_seq.get(job_id, -1))

        if len(self.data_flat) > self.max_total_cache_size:
            print('MDP cache overflow')
            ((job_id, chunk_id,flow_id), t) = self.data_flat.popitem(last=False)
            del self.data[job_id][chunk_id]
            if chunk_id == self.max_chunk_seq[job_id]:
                if len(self.data[job_id]) == 0:
                    self.max_chunk_seq[job_id] = -1
                else:
                    self.max_chunk_seq[job_id] = max(self.data[job_id].keys())

    def get_relocation_seq(self, job_id, chunk_id):
        return self.max_chunk_seq[job_id]

    def __len__(self):

        return len(self.data_flat)

    def __delitem__(self, key):
        job_id, chunk_id,flow_id = key
        keys_to_remove = []
   
        for k in self.data_flat.keys():        
            if k[0] == job_id and k[1] == chunk_id:
                keys_to_remove.append(k)
        for k in keys_to_remove:
            del self.data_flat[k]
            
        del self.data[job_id][chunk_id]
        if len(self.data[job_id]) == 0:
            if chunk_id == self.max_chunk_seq[job_id]:
                self.max_chunk_seq[job_id] = -1
            del self.data[job_id]
        else:
            if chunk_id == self.max_chunk_seq[job_id]:
                self.max_chunk_seq[job_id] = max(self.data[job_id].keys())  


class EdgeBox(Middlebox):
    STOPPED = 0
    RUNNING = 1

    def __init__(
            self,
            max_mup_cache_size=100000,
            mup_timeout=10,
            enabled=True,
            jobs_config={},
            relocation_enabled=False):
        super().__init__()
        self.nfs = {}
        self.nfs[INA_PAATP.TYPE] = self.process_paatp
        self.nfs[DCTCP.TYPE] = self.process_dctcp
        self.nfs[TCP_Reno.TYPE] = self.process_tcp
        self.nfs[ATP.TYPE] = self.process_atp

        self.mup_cache = MDPCache(max_mup_cache_size) 
        self.mup_cache_meta = {}
        self.mup_ece = {}
        self.mup_perf_metrics = dict(
            total=0,
            detail=dict(timeout=0, overflow=0, completed=0),
        )

        self.max_mup_cache_size = max_mup_cache_size

        self.mup_timeout = mup_timeout

        self.flows_of_each_job = {}
        self.jobs_config = jobs_config

        self.enabled = enabled
        self.perf_metrics = dict(sent={}, received={})

        self.remove_timeout_mupchunk_running = False
        self.relocation_enabled = relocation_enabled

    def stop(self):
        self.state = self.STOPPED

    def process(self, pkt):
        
        self.perf_metrics['received'][pkt.pkt_type] = self.perf_metrics['received'].get(pkt.pkt_type, 0) + 1
        if self.enabled:
            pkts = self.nfs[pkt.flow.TYPE](pkt)
        for pkt in pkts:
            self.perf_metrics['sent'][pkt.pkt_type] = self.perf_metrics['sent'].get(pkt.pkt_type, 0) + 1
        return pkts


    
    def change_ping_to_cwd(self, pkt,Ssum):
        return pkt.flow.change_ping_to_cwd(pkt,Ssum)


    def register_net(self, net):
        self.net = net 

    def enable_remove_timeout_mupchunk(self):
        if self.remove_timeout_mupchunk_running:
            return
        else:
            events = self.remove_timeout_mupchunk()
            self.net.add_events(events)

    def remove_timeout_mupchunk(self):
      
        cur_time = self.get_cur_time()
        to_remove = []
        t_threshold = cur_time - self.mup_timeout

        for chunk_key, cache_entry in list(self.mup_cache.data_flat.items()):
            if cache_entry < t_threshold:
                to_remove.append(chunk_key)
            else:
                break
        for k in to_remove:
            del self.mup_cache.data_flat[k]
            del self.mup_cache.data[k[0]][k[1]] 
            # self.add_aggregated_to_send_queue(chunk_key=k, reason='timeout')

        new_events = []
        if len(self.mup_cache.data_flat) == 0:
            self.remove_timeout_mupchunk_running = False
        else:
     
            e = Event(cur_time + self.mup_timeout, self, "remove_timeout_mupchunk")
            new_events.append(e)

        return new_events

    def process_paatp(self, pkt):
        
        job_id = pkt.flow.job_id
        flow_id = pkt.flow.id

        chunk_key = (job_id, pkt.chunk_seq)

        cur_time = self.get_cur_time()
        pkts = []
        if pkt.pkt_type == INA_PAATP.PING_PKT_TYPE:

            if pkt.ecn==Packet.CE:
                pkt.ece = True 
            if pkt.resend:
                if self.mup_cache.has_cache(job_id, pkt.chunk_seq,flow_id):
                    for fid in self.mup_cache_meta[chunk_key]:
                        pkt.bitmap[fid] = 1
                        
                    pkt.bitmap[pkt.flow.id] = 1
                            
                    del self.mup_cache[job_id, pkt.chunk_seq,flow_id]
                    del self.mup_cache_meta[chunk_key]
                else:
                    pass
                pkts.append(pkt)

            else:
                if flow_id not in self.mup_cache_meta.get(chunk_key, []):

                    self.mup_cache_meta.setdefault(chunk_key, set()).add(flow_id)

                    if not self.mup_cache.has_cache(job_id, pkt.chunk_seq,flow_id):
                        self.mup_cache.update_cache(job_id, pkt.chunk_seq,flow_id, cur_time)
                        self.enable_remove_timeout_mupchunk()

                        #create cwd
                        flow_for_id = next((f for f in self.net.named_flows.values() if (f.id == pkt.flow.id and f.TYPE == pkt.flow.TYPE)), None)
                        if(flow_for_id == None):
                            raise ValueError
                        else:
                            Ssum = self.mup_cache.update_gradient_seq(job_id,flow_id, pkt.chunk_seq)
                            awd_pkt = Packet(sent_time=cur_time,
                                        priority= pkt.priority,
                                        pkt_type=flow_for_id.CWD_PKT_TYPE,
                                        size_in_bits=flow_for_id.CWD_PKT_SIZE_IN_BITS,
                                        flow=flow_for_id,
                                        ecn=pkt.ecn,
                                        ece=pkt.ece,
                                        path=[flow_for_id.reversed_path[-2], flow_for_id.reversed_path[-1]],

                            )
                            awd_pkt.hop_cnt = 0
                            awd_pkt.ecn == pkt.ecn
                            awd_pkt.ece = pkt.ece

                            awd_pkt.ping_path = flow_for_id.path
                            awd_pkt.ack_seq = pkt.ack_seq    

                            awd_pkt.chunk_seq = pkt.chunk_seq      
                            awd_pkt.recv_time = pkt.recv_time

                            awd_pkt.Ssum = Ssum
                            pkts.append(awd_pkt)
                    else:
                        pass

                    if len(self.mup_cache_meta[chunk_key]) == self.jobs_config[job_id]['flownum']:
                        for fid in self.mup_cache_meta[chunk_key]:
                            pkt.bitmap[fid] = 1
                        pkts.append(pkt)

        elif pkt.pkt_type == INA_PAATP.PONG_PKT_TYPE:
            if (len( self.mup_cache_meta) >= self.net.line):
                pkt.aecn = True

            if(pkt.multicast):
                flows_for_job_i = [f for f in self.net.named_flows.values() if (f.job_id == pkt.flow.job_id and f.TYPE == pkt.flow.TYPE)]
                for flow in flows_for_job_i:
                    new_pkt = Packet(sent_time=flow.send_times[pkt.chunk_seq],
                                priority=flow.get_pkt_priority(),
                                pkt_type=flow.PONG_PKT_TYPE,
                                size_in_bits=flow.PONG_PKT_SIZE_IN_BITS,
                                flow=flow,
                                ecn=pkt.ecn,
                                ece=pkt.ece,
                                path=flow.reversed_path,
                    )
                    new_pkt.hop_cnt = pkt.hop_cnt
                    new_pkt.ecn == pkt.ecn
                    new_pkt.ece = pkt.ece

                    new_pkt.ping_path = flow.path
                    new_pkt.ack_seq = pkt.ack_seq    

                    new_pkt.chunk_seq = pkt.chunk_seq      
                    new_pkt.recv_time = pkt.recv_time

                    new_pkt.aecn =  pkt.aecn
                    pkts.append(new_pkt)

                del self.mup_cache[job_id, pkt.chunk_seq,flow_id]
                del self.mup_cache_meta[chunk_key]

            else:
                pkts.append(pkt)   

        else:
            raise ValueError
        return pkts
    
    def process_atp(self, pkt):
        job_id = pkt.flow.job_id
        flow_id = pkt.flow.id

        chunk_key = (job_id, pkt.chunk_seq)

        cur_time = self.get_cur_time()
        pkts = []
        if pkt.pkt_type == ATP.PING_PKT_TYPE:
            if pkt.ecn==Packet.CE:
                self.mup_ece[chunk_key] = True 
            if pkt.resend:
                if self.mup_cache.has_cache(job_id, pkt.chunk_seq,flow_id):
                    if chunk_key in self.mup_ece and self.mup_ece[chunk_key]:
                        pkt.ece = True
                        pkt.ecn = pkt.ECT

                    for fid in self.mup_cache_meta[chunk_key]:
                        pkt.bitmap[fid] = 1
                        
                    pkt.bitmap[pkt.flow.id] = 1
                            
                    del self.mup_cache[job_id, pkt.chunk_seq,flow_id]
                    del self.mup_cache_meta[chunk_key]
                    if chunk_key in self.mup_ece:
                        del self.mup_ece[chunk_key] 
                else:
                    pass
                pkts.append(pkt)   
            else:
                if flow_id not in self.mup_cache_meta.get(chunk_key, []):
                    self.mup_cache_meta.setdefault(chunk_key, set()).add(flow_id)
                    if not self.mup_cache.has_cache(job_id, pkt.chunk_seq,flow_id):
                        self.mup_cache.update_cache(job_id, pkt.chunk_seq,flow_id, cur_time)
                        self.enable_remove_timeout_mupchunk()
                    else:
                        pass

                    if len(self.mup_cache_meta[chunk_key]) == self.jobs_config[job_id]['flownum']:
                        if  chunk_key in self.mup_ece and self.mup_ece[chunk_key]:
                            pkt.ece = True
                            pkt.ecn = pkt.ECT
                        for fid in self.mup_cache_meta[chunk_key]:
                            pkt.bitmap[fid] = 1
                        pkts.append(pkt)
                    

        elif pkt.pkt_type == ATP.PONG_PKT_TYPE:
            if(pkt.multicast):
                flows_for_job_i = [f for f in self.net.named_flows.values() if (f.job_id == pkt.flow.job_id and f.TYPE == pkt.flow.TYPE)]
                for flow in flows_for_job_i:
                    new_pkt = Packet(sent_time=flow.send_times[pkt.chunk_seq],
                                priority=flow.get_pkt_priority(),
                                pkt_type=flow.PONG_PKT_TYPE,
                                size_in_bits=flow.PONG_PKT_SIZE_IN_BITS,
                                flow=flow,
                                ecn=pkt.ecn,
                                ece=pkt.ece,
                                path=flow.reversed_path,
                    )
                    new_pkt.hop_cnt = pkt.hop_cnt
                    new_pkt.ecn == pkt.ecn
                    new_pkt.ece = pkt.ece

                    new_pkt.ping_path = flow.path
                    new_pkt.ack_seq = pkt.ack_seq    

                    new_pkt.chunk_seq = pkt.chunk_seq      
                    new_pkt.recv_time = pkt.recv_time
                    pkts.append(new_pkt)

                del self.mup_cache[job_id, pkt.chunk_seq,flow_id]
                del self.mup_cache_meta[chunk_key]
                if chunk_key in self.mup_ece:
                    del self.mup_ece[chunk_key] 
            else:
                pkts.append(pkt)   
            

        else:
            raise ValueError
        return pkts
    
    def process_dctcp(self, pkt):
        return [pkt]
    
    def process_tcp(self, pkt):
        return [pkt]






if __name__ == '__main__':
    print('done!')
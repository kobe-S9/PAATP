import argparse

import matplotlib.pyplot as plt
import numpy as np

from core import *
from protocols import *
import os
import pickle


import math

parser = argparse.ArgumentParser(description='hello.')
parser.add_argument('--init-sending-rate-pps', type=int, default=10)
parser.add_argument('--num', type=int, required=False, default=5000,help='gradient_num')
parser.add_argument('--jobnum', type=int, required=False, default=1,help='jobnum')
parser.add_argument('--dnum', type=int, required=False, default=2,help='DeviceNum')
parser.add_argument('--FCLS', type=str, required=False, default='PAATP',help='FCLS')
parser.add_argument('--pname', type=str, default='default_plot', help='Name of the plot directory')

args = parser.parse_args()


inst = None

_fig = lambda : plt.figure(figsize=(5, 3.5)).add_subplot(111)
axs = {}


def mkAXS(key=0):
    ax = _fig()
    axs[key] = ax
    return ax

lw = 2
fontsize = 14
M = 7


def show(plotname):

    print(f"Total number of axes: {len(axs)}")
    for name, ax in axs.items():

        plt.figure()
        plt.sca(ax)
        ticklines = ax.xaxis.get_ticklines()
        ticklabels = ax.xaxis.get_ticklabels()
        for tk, tkl in zip(ticklines, ticklabels):
            #    tk.set_markersize(12)
            tkl.set_fontsize(14)
        ticklines = ax.yaxis.get_ticklines()
        ticklabels = ax.yaxis.get_ticklabels()
        for tk, tkl in zip(ticklines, ticklabels):
            #    tk.set_markersize(12)
            tkl.set_fontsize(14)
        #plt.legend()#(loc='best')
        plt.tight_layout()
        os.makedirs(f'plots/{plotname}', exist_ok=True)
        plt.savefig(f'plots/{plotname}/{ax.get_title()}.png')

        plt.close()


    plt.show()



class Demo(object):
    def __init__(self,
            DeviceNum=1,
            JobNum=1,
            bw_bps=1e9,#1e9 为1Gbps
            rtt = 1e-6 ,#1e-6为1us
            loss=0,
            FCLS=TCP_Reno,
            total_chunk_num=50,
            queue_lambda=1.,
            max_mup_cache_size=4000,
            enabled_box=True,
            relocation_enabled=False,):

        self.link_bw_bps = bw_bps
        self.loss_rate = loss
        self.queue_lambda = queue_lambda
        ideal_rtt = rtt
        lat = ideal_rtt / 4
        bdp_in_bits = int(bw_bps * ideal_rtt)
        qsize_in_bits = max(80 * 8e3, bdp_in_bits * 2)
        # qsize_in_bits =  bdp_in_bits * 2

        weights = {1: 8, 2: 4, 3: 2, 4: 1}
        mq_sizes = {i: qsize_in_bits for i in weights}
        ecn_threshold_factors = {i: 0.3 for i in mq_sizes}
        qdisc = DWRR(mq_sizes, ecn_threshold_factors, weights=weights)

        mup_jobs_config = {}
        ps_job_record = {}
        for i in range(JobNum):
            mup_jobs_config[i] = dict(flownum=DeviceNum)
            ps_job_record[i] = {}
   
        mup_jobs_config[2] = dict(flownum=1) #backflow
        self.links = [
            Link(bw_bps, lat, qdisc.copy(), loss) 
            for _ in range(2 * (DeviceNum * JobNum + JobNum))
        ]
        self.boxes = [
            EdgeBox(
                jobs_config=mup_jobs_config,
                max_mup_cache_size=max_mup_cache_size,
                enabled=enabled_box,
                relocation_enabled=relocation_enabled,
            ),
        ]

        self.flows = []
        print(self.links)
        
        count = 0
        Jobcount = 0
        for i in range(JobNum):
            for j in  range(DeviceNum):
                
                f = FCLS(
                    path=[self.links[count],self.boxes[0] ,self.links[-(Jobcount+2)], ], 
                    rpath=[self.links[-(Jobcount+1)],self.boxes[0] ,self.links[count+1], ],
                    nic_rate_bps=0.25* bw_bps, 
                    job_id=i,
                    Q = j + 1,
                    #start_time=0, 
                    #expired_time=20,
                    total_chunk_num=total_chunk_num,
                    timeout_threshold=1,
                    init_cwnd=15,
                    stats=[],
                )
                if j != 0:
                    f.path[0] = self.links[2]
        

                print("f.path",f.path[0].id,f.path[2].id)
                print("f.rpath",f.reversed_path[0].id,f.reversed_path[2].id)
                self.flows.append(f)
                count += 2

            Jobcount += 2
        
        for _ in range(3):
          self.links.append(Link(bw_bps, lat, qdisc.copy(), loss))

        # backflow = ATP(
        #         path=[self.links[0],self.boxes[0] ,self.links[-1], ], 
        #         rpath=[self.links[-2],self.boxes[0] ,self.links[-3], ],
        #         nic_rate_bps=1*bw_bps, #2*
        #         job_id= 2,
        #         start_time=0.003, 
        #         #expired_time=20,
        #         total_chunk_num=total_chunk_num/3,
        #         timeout_threshold=1,
        #         init_cwnd=15,
        #         stats=[],
        #         )
        # self.flows.append(backflow)
        # print("backflow.path",backflow.path[0].id,backflow.path[2].id)
        # print("backflow.rpath",backflow.reversed_path[0].id,backflow.reversed_path[2].id)
        # self.flows[1].Q = 2
        self.net = Network(links=self.links,boxes=self.boxes, flows=self.flows)
        for i in range(JobNum):
            flows_for_job_i = [f for f in self.net.named_flows.values() if f.job_id == i ]
            for flow in flows_for_job_i:       
                ps_job_record[i][flow.id] = 1
            self.net.awd[i] = 15
            self.net.ack_pkt_cnt[i] = 0
            self.net.aecn_pkt_cnt[i] = 0
        
        # ps_job_record[2] = {} #backflow
        # ps_job_record[2][backflow.id] = 1
        self.net.ps_job_record = ps_job_record
        self.net.jobs_config =mup_jobs_config

               
        #PAATP: ACEN
        #CA: the total aggregation throughput ,equal to the sum of most stalled workers' rate of all jobs.
        # ideal_rtt =4* PAATP.PING_PKT_SIZE_IN_BITS / bw_bps
        most_stalled_rate = 1.5*bw_bps
        CA = most_stalled_rate * ideal_rtt /Multi.PONG_PKT_SIZE_IN_BITS * JobNum
        CA_and_N = CA + JobNum

        M_and_BDP = max_mup_cache_size + (bw_bps*ideal_rtt*JobNum)/Multi.PONG_PKT_SIZE_IN_BITS

        Eta = 1 - CA_and_N / M_and_BDP
        self.net.line = Eta * M_and_BDP
        print("SZX CA_and_N:",CA_and_N)
        print("SZX M_and_BDP:",M_and_BDP)
        print("self.Eta:",Eta)
        self.net.line = 30000

        print("SZX ACEN line:",self.net.line)
        # q = 4
        # Multi.set_pong_pkt_size(q)

        events = [
         
            #  Event(0.0, self.flows[1], 'update_rate', params=dict(version=0, new_rate_bps = most_stalled_rate)),
            # Event(0.0, self.flows[0], 'update_Q', params=dict(Q=q)),

            # Event(0.0, self.flows[0], 'update_rate', params=dict(version=0, new_rate_bps=0.1 * bw_bps)),
            # Event(0.0, self.flows[1], 'update_rate', params=dict(version=0, new_rate_bps=0.5 * bw_bps )),
            # Event(0.0, self.flows[2], 'update_rate', params=dict(version=0, new_rate_bps=1 * bw_bps )),
            # Event(0.0, self.flows[3], 'update_rate', params=dict(version=0, new_rate_bps=1.5 * bw_bps )),

            # Event(0.5, self.flows[0], 'update_rate', params=dict(version=0, new_rate_bps=4*bw_bps )),
   

        ]
        self.net.add_events(events)



    def runsim(self):
        self.net.run()

    def plot(self,plotname):
        #print(self.stat)
        spec_set = set()
        ax_dict = {}
        output = {}
        plot_data = {}
        for f in self.flows: 
            print(
                'flow', f.id, 
                'sent_ping', f.sent_ping, 'received_ping', f.received_ping,
                'sent_pong', f.sent_pong, 'received_pong', f.received_pong
                )

            print('drop stat', f.lost_reason)
            print('expired time', f.expired_time)
            
            for t, stat in f.stats:# f.stats是有元组构成的数组，t为时间，stat为流的状态（一个字典，通过get_stat()得到）
                for k, v in stat.items(): # 设置键k对应的值为v
                    if k not in plot_data:
                        plot_data[k] = {}
                    if f.id not in plot_data[k]:
                        plot_data[k][f.id] = []
                    if isinstance(v, list):
                        plot_data[k][f.id].extend(v)
                        spec_set.add(k)
                    else:  # 只有数值得属性需要添加时间信息
                        plot_data[k][f.id].append((t, v))

        for l in self.links: 
            for t, stat in l.stats:# f.stats是有元组构成的数组，t为时间，stat为流的状态（一个字典，通过get_stat()得到）
                for k, v in stat.items(): # 设置键k对应的值为v
                    if k not in plot_data:
                        plot_data[k] = {}
                    if l.id not in plot_data[k]:
                        plot_data[k][l.id] = []
                    if isinstance(v, list):
                        plot_data[k][l.id].extend(v)
                        spec_set.add(k)
                    else:  # 只有数值得属性需要添加时间信息
                        plot_data[k][l.id].append((t, v))
        

        for b in self.boxes: 
            for t, stat in b.stats:# f.stats是有元组构成的数组，t为时间，stat为流的状态（一个字典，通过get_stat()得到）
                for k, v in stat.items(): # 设置键k对应的值为v
                    if k not in plot_data:
                        plot_data[k] = {}
                    if b.id not in plot_data[k]:
                        plot_data[k][b.id] = []
                    if isinstance(v, list):
                        plot_data[k][b.id].extend(v)
                        spec_set.add(k)
                    else:  # 只有数值得属性需要添加时间信息
                        plot_data[k][b.id].append((t, v))
        
        

        
        # print(list(plot_data))
        for k in plot_data:
            #if 'ecn' in k:
            #    continue
            #if k not in spec_set:
            ax = mkAXS(key=k)
            ax.set_title(k)
            for fid in plot_data[k]:
                x, y = [], []  # x为时间，y为数值
                for xx, yy in plot_data[k][fid]:
                    x.append(xx)
                    y.append(yy)
                ax.errorbar(x=x, y=y, label='flow {0}'.format(fid))
        

            ax.set_ylabel(k)
            ax.legend()

        os.makedirs(f'plot_data/{plotname}', exist_ok=True)
        with open(f'plot_data/{plotname}/plot_data.pkl', 'wb') as f:
            pickle.dump(plot_data, f)
    

def convert_to_rate(xy, stepsize=10):
    x = []
    y = []
    last_xx, last_yy = xy[0]
    for i, (xx, yy) in enumerate(xy):
        if i >0 and i % stepsize == 0:
            x.append(xx)
            y.append((yy - last_yy) / (xx - last_xx))
            last_xx = xx
            last_yy = yy
    return x, y


if __name__ == '__main__':
    for FCLS in (PingPongFlow, TCP_Reno)[-1:]:#, DCTCP, DCTCPi)[-1:]:
        inst = Demo(DeviceNum=args.dnum, JobNum=args.jobnum, FCLS=eval(args.FCLS), total_chunk_num=args.num,
                     bw_bps=1e8, rtt=1e-3, loss=0.0, max_mup_cache_size=40000, enabled_box=True, relocation_enabled=False)
        inst.runsim()

        print()

        for f in inst.flows:
            print('# Flow', f.__class__.__name__, f.id, "sent ping, received ping, sent pong, recevied pong", f.sent_ping, f.received_ping, f.sent_pong, f.received_pong, f.last_pong_received_time)
            if f.TYPE == 'Multi':
                print('flow.cwd', f.received_cwd)
                print('flow.ping_pkt_size',f.PING_PKT_SIZE_IN_BITS)
                print('flow.pong_pkt_size',f.PONG_PKT_SIZE_IN_BITS)
            # else:
            #     print(f.TYPE)
        inst.plot(args.pname)
    show(args.pname)

    print('done!')
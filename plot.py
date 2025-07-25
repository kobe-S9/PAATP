import pickle
import matplotlib.pyplot as plt
import os
import matplotlib.cm as cm

def load_plot_data(runname):
    path = f'plot_data/{runname}/plot_data.pkl'
    with open(path, 'rb') as f:
        return pickle.load(f)
def merge_and_plot(runnames):
    merged_data = {}  # k -> runname -> id -> [(t,v)]

    # 先加载所有数据
    for runname in runnames:
        data = load_plot_data(runname)
        for k, v in data.items():
            if k not in merged_data:
                merged_data[k] = {}
            merged_data[k][runname] = v

    # 合并所有 link 开头指标到一个新指标 'link_total_utilization'
    merged_data['link_total_utilization'] = {}

    for runname in runnames:
        merged_data['link_total_utilization'][runname] = {}

        all_vals = []
        # 遍历所有以link开头的k
        for k in merged_data:
            if k.startswith('link'):
                id_dict = merged_data[k].get(runname, {})
                for id_, val_list in id_dict.items():
                    for _, v in val_list:
                        all_vals.append(v)
        # 把所有link指标数据合成一个虚拟id（比如'combined')
        if all_vals:
            avg_val = sum(all_vals) / len(all_vals)
            merged_data['link_total_utilization'][runname]['combined'] = [(0, avg_val)]
        else:
            merged_data['link_total_utilization'][runname]['combined'] = []

    # 绘图时，跳过所有 link 开头指标，只画 link_total_utilization 作为总链路利用率
    keys_to_skip = [k for k in merged_data if k.startswith('link')]

    for k, run_data in merged_data.items():
        if k in keys_to_skip and k != 'link_total_utilization':
            continue  # 跳过单独的link指标

        plt.figure(figsize=(10, 6))
        plt.title('总链路利用率' if k == 'link_total_utilization' else k)

        if k in ["completed_time", "send_for_throughput", "received_pong_for_throughput", "received_cwd_for_throughput", "sw_agg_count", 'link_total_utilization']:
            bars = []
            labels = []
            colors = []
            colormap = cm.get_cmap('tab10')

            for idx, (runname, id_dict) in enumerate(run_data.items()):
                color = colormap(idx % 10)
                for id_, val_list in id_dict.items():
                    if val_list:
                        avg_val = sum(v for _, v in val_list) / len(val_list)
                        bars.append(avg_val)
                        labels.append(f"{runname}-{id_}" if k != 'link_total_utilization' else runname)
                        colors.append(color)

            plt.bar(labels, bars, color=colors)
            plt.ylabel('link_total_utilization' if k == 'link_total_utilization' else k)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            for i, v in enumerate(bars):
                plt.text(i, v + 0.01, f"{v:.2f}", ha='center', va='bottom', fontsize=10)
        else:
            plt.xlabel("Time")
            plt.ylabel(k)
            for runname, id_dict in run_data.items():
                for id_, values in id_dict.items():
                    x = [t for t, _ in values]
                    y = [val for _, val in values]
                    plt.plot(x, y, label=f'{runname}-{id_}')
            plt.legend()

        os.makedirs('plots/merged', exist_ok=True)
        plt.savefig(f'plots/merged/{k}.png')
        plt.close()

if __name__ == "__main__":
    runnames = [ 'QAINA-2','QAINA-3','QAINA-4','QAINA-5','QAINA-6','QAINA-7','QAINA-8']  # 多次运行的标识
    merge_and_plot(runnames)

#!/usr/bin/env python3

import sys
import argparse

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import hashlib

common_schemes = {
     'puffer_ttp_cl/bbr': ['Fugu', 'tab:red'], 
     'linear_bba/bbr': ['BBA', 'tab:green'], 
     'pensieve/bbr': ['Pensieve', 'tab:purple'], 
     'pensieve_in_situ/bbr': ['Pensieve (Puffer traces)', 'tab:pink'],
     'mpc/bbr': ['MPC-HM', 'tab:blue'], 
     'robust_mpc/bbr': ['RobustMPC-HM', 'tab:brown'], 
     'puffer_ttp_20190202/bbr': ['Fugu-Feb', 'tab:orange'], 
     'puffer_ttp_20190302/bbr': ['Fugu-Mar', 'tab:olive'],
     'puffer_ttp_20190402/bbr': ['Fugu-Apr', 'tab:cyan'], 
     'puffer_ttp_20190502/bbr': ['Fugu-May', 'tab:gray'],
     'fugu_variant_cl/bbr': ['Memento', '#0f6c44'],
     'fugu_variant_cl3/bbr': ['Memento-v3a', '#bc61f5'],
     'fugu_variant_cl4/bbr': ['Memento-v3b', '#461257'],
}

# Keep name => color mapping consistent across experiments and runs.
# No matplotlib colormaps to avoid duplicating colors.
def get_color(name):
    # Assign static colors to the common schemes. 
    # For others, hash the name.
    if name in common_schemes:
        return common_schemes[name][1]
    else:
        # hashlib sha is deterministic unlike hash() 
        sha = hashlib.sha256()      
        sha.update(name.encode())
        return '#' + sha.hexdigest()[:6]

def plot_data(data, output_figure, title):
    fig, ax = plt.subplots()
    ax.set_xlabel('Time spent stalled (%)')
    ax.set_ylabel('Average SSIM (dB)')

    for name in data:
        if name == 'nstreams' or name == 'nwatch_hours':
            continue

        x = data[name]['stall'][2]
        y = data[name]['ssim'][2]
        
        pretty_name = name if name not in common_schemes else common_schemes[name][0]
        pretty_color = get_color(name)
        
        ax.scatter(x, y, color=pretty_color, label=pretty_name)
        ax.errorbar(x, y,
            xerr=[[x - data[name]['stall'][0]], [data[name]['stall'][1] - x]],
            yerr=[[y - data[name]['ssim'][1]], [data[name]['ssim'][0] - y]],
            ecolor=pretty_color,
            capsize=4.0)
        # Labels are often very overlapping -- try legend
        #ax.annotate(pretty_name, (x, y),
        #            xytext=(4,5), textcoords='offset pixels')
        ax.legend()

    subtitle = '{} streams, {:.0f} stream-hours'.format(data['nstreams'], data['nwatch_hours'])
    plt.title(title + '\n(' + subtitle + ')' if title else subtitle)
    ax.invert_xaxis()

    # Hide the right and top spines
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    fig.savefig(output_figure)
    sys.stderr.write('Saved plot to {}\n'.format(output_figure))


def parse_data(input_data_path):
    data = {}

    with open(input_data_path) as fh:
        nstreams = 0
        nwatch_hours = 0

        for line in fh:
            if line[0] == '#':
                items = line.split()
                nstreams += int(items[2])
                nwatch_hours += float(items[-1].split('/')[1])
                continue
            
            line = line.replace(',', '').replace(';', '').replace('%', '')

            items = line.split()

            name = items[0]
            data[name] = {}

            # stall_low, stall_high, stall_mean
            data[name]['stall'] = [float(x) for x in items[5:11:2]]

            # ssim_low, ssim_high, ssim_mean
            data[name]['ssim'] = [float(x) for x in items[13:19:2]]

    data['nstreams'] = nstreams
    data['nwatch_hours'] = nwatch_hours

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_data', help='input data (output of confint)', required=True)
    parser.add_argument('--output_figure', help='output figure', required=True)
    parser.add_argument('--title')
    args = parser.parse_args()

    data = parse_data(args.input_data)
    plot_data(data, args.output_figure, args.title)


if __name__ == '__main__':
    main()

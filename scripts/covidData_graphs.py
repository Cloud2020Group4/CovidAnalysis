import covidData, utils
import matplotlib.pyplot as plt
import matplotlib.figure as fig
import numpy as np

def plot_dataframe_with_date(df, columnX, columnY, name, save_name, xlabel = None, ylabel = None):
    plt.close('all')
    x_values = df.select(columnX).collect()
    y_values = df.select(columnY).collect()
    fig, ax = plt.subplots()
    ax.plot(x_values, y_values)
    fig.autofmt_xdate()
    ax.set_title(name)
    if xlabel != None:
        ax.set_xlabel(xlabel)
    if ylabel != None:
        ax.set_ylabel(ylabel)
    plt.savefig(save_name)

def plot_dataframe_with_date_double(df, columnX, columnY1, columnY2, name, save_name, label1, label2, xlabel = None, ylabel = None):
    plt.close('all')
    x_values = df.select(columnX).collect()
    y1_values = df.select(columnY1).collect()
    y2_values = df.select(columnY2).collect()
    fig, ax = plt.subplots()
    ax.plot(x_values, y1_values, label = label1)
    ax.plot(x_values, y2_values, label = label2)
    plt.legend()
    fig.autofmt_xdate()
    ax.set_title(name)
    if xlabel != None:
        ax.set_xlabel(xlabel)
    if ylabel != None:
        ax.set_ylabel(ylabel)
    plt.savefig(save_name)

def plot_bars(df, bar_names, bar_values, name, save_name):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [row[0] for row in df.select(bar_names).collect()]
    values = [row[0] for row in df.select(bar_values).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values)
    plt.xticks(pos, bars)
    ax.set_title(name)
    fig.autofmt_xdate()
    plt.savefig(save_name)

def plot_bars_months(df, bar_names, bar_values, name, save_name):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [utils.month_string(row[0]) for row in df.select(bar_names).collect()]
    values = [row[0] for row in df.select(bar_values).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values)
    plt.xticks(pos, bars)
    ax.set_title(name)
    fig.autofmt_xdate()
    plt.savefig(save_name)

def plot_bars_months_double(df, bar_names, bar_values1, bar_values2, name, save_name, label1, label2):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [utils.month_string(row[0]) for row in df.select(bar_names).collect()]
    values1 = [row[0] for row in df.select(bar_values1).collect()]
    values2 = [row[0] for row in df.select(bar_values2).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values1, label = label1, width=0.4)
    ax.bar(pos + 0.4, values2, label= label2, width=0.4)
    plt.xticks(pos + 0.2, bars)
    ax.set_title(name)
    fig.autofmt_xdate()
    plt.legend()
    plt.savefig(save_name)
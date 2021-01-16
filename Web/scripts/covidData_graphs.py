import covidData, utils

# For being able to use external python libraries, set [export PYSPARK_PYTHON='/usr/bin/python']
# Also you have to install the library before (pip install <library>)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os
from pyspark.sql.functions import col

def plot_dataframe_with_date(df, columnX, columnY, name, save_name, xlabel = None, ylabel = None):
    plt.close('all')
    df.show()
    x_values = [row[0] for row in df.select(columnX).collect()]
    y_values = [row[0] for row in df.select(columnY).collect()]
    fig, ax = plt.subplots()
    ax.plot(x_values, y_values)
    ax.xaxis.set_major_locator(ticker.AutoLocator())
    fig.autofmt_xdate()
    ax.set_title(name, loc='center', wrap=True)
    if xlabel != None:
        ax.set_xlabel(xlabel)
    if ylabel != None:
        ax.set_ylabel(ylabel)
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)

def plot_dataframe_with_date_double(df, columnX, columnY1, columnY2, name, save_name, label1, label2, xlabel = None, ylabel = None):
    plt.close('all')
    x_values = [row[0] for row in df.select(columnX).collect()]
    y1_values = [row[0] for row in df.select(columnY1).collect()]
    y2_values = [row[0] for row in df.select(columnY2).collect()]
    fig, ax = plt.subplots()
    ax.plot(x_values, y1_values, label = label1)
    ax.plot(x_values, y2_values, label = label2)
    ax.xaxis.set_major_locator(ticker.AutoLocator())
    plt.legend()
    fig.autofmt_xdate()
    ax.set_title(name, loc='center', wrap=True)
    if xlabel != None:
        ax.set_xlabel(xlabel)
    if ylabel != None:
        ax.set_ylabel(ylabel)
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)

def plot_bars(df, bar_names, bar_values, name, save_name):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [row[0] for row in df.select(bar_names).collect()]
    values = [row[0] for row in df.select(bar_values).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values)
    plt.xticks(pos, bars)
    ax.set_title(name, loc='center', wrap=True)
    fig.autofmt_xdate()
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)

def plot_three_bars_continent(df, bar_names, bar_values1, bar_values2, bar_values3, name, save_name, label1, label2, label3):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [row[0] for row in df.select(bar_names).collect()]
    values1 = [row[0] for row in df.select(bar_values1).collect()]
    values2 = [row[0] for row in df.select(bar_values2).collect()]
    values3 = [row[0] for row in df.select(bar_values3).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos + 0.0, values1, label = label1, width=0.25)
    ax.bar(pos + 0.25, values2, label= label2, width=0.25)
    ax.bar(pos + 0.5, values3, label= label3, width=0.25)
    plt.xticks(pos + 0.25, bars)
    ax.set_title(name, loc='center', wrap=True)
    plt.legend()
    fig.autofmt_xdate()
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)    

def plot_bars_months(df, bar_names, bar_values, name, save_name):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [utils.year_month_string(row[0]) for row in df.select(bar_names).collect()]
    values = [row[0] for row in df.select(bar_values).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values)
    plt.xticks(pos, bars)
    ax.set_title(name, loc='center', wrap=True)
    fig.autofmt_xdate()
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)

def plot_bars_months_double(df, bar_names, bar_values1, bar_values2, name, save_name, label1, label2):
    plt.close('all')
    fig, ax = plt.subplots()
    bars = [utils.year_month_string(row[0]) for row in df.select(bar_names).collect()]
    values1 = [row[0] for row in df.select(bar_values1).collect()]
    values2 = [row[0] for row in df.select(bar_values2).collect()]
    pos = np.arange(len(bars))
    ax.bar(pos, values1, label = label1, width=0.4)
    ax.bar(pos + 0.4, values2, label= label2, width=0.4)
    plt.xticks(pos + 0.2, bars)
    ax.set_title(name, loc='center', wrap=True)
    fig.autofmt_xdate()
    plt.legend()
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)

def plot_pie(df, labels, values, name, save_name):
    plt.close('all')
    labels_list = [row[0] for row in df.select(labels).collect()]
    values_list = [row[0] for row in df.select(values).collect()]
    plt.pie(values_list, labels = labels_list, startangle=90, autopct='%.2f')
    plt.axis('equal')
    plt.title(name, loc='center', wrap=True)
    if os.path.exists(save_name):
        os.remove(save_name)
    plt.savefig(save_name)
    print('Graph saved at ' + save_name)
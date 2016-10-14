import multiprocessing as mp

import numpy as np
import pandas as pd
from Queue import Empty
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from datetime import datetime
from sqlalchemy import create_engine
import pickle

from DatabaseConnections.PostgresConnection import postgres_cursor
from DatabaseConnections.VerticaConnection import vertica_cursor
from GetAdjustedPrices import SORTED_COLLECTION_PRICES

cluster = Cluster(
    ['s-msk-p-fxa-cs1', 's-msk-p-fxa-cs2', 's-msk-p-fxa-cs3', 's-msk-p-fxa-cs4'],
    auth_provider=(PlainTextAuthProvider(username='efxtradeuser', password='EfxTradeUserPWD')))
session = cluster.connect('efxtradekspace')


def lowpriority():
    """ Set the priority of the process to below-normal."""
    import sys
    try:
        sys.getwindowsversion()
        isWindows = True
    except:
        isWindows = False

    if isWindows:
        # Based on:
        #   "Recipe 496767: Set Process Priority In Windows" on ActiveState
        #   http://code.activestate.com/recipes/496767/
        import win32api, win32process, win32con
        pid = win32api.GetCurrentProcessId()
        handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
        win32process.SetPriorityClass(handle, win32process.BELOW_NORMAL_PRIORITY_CLASS)
    else:
        import os
        os.nice(1)


class VerticaCassandraPricePush:
    def __init__(self, nCPU):
        self.nCPU = nCPU
        self.local_engine = create_engine('postgresql://dbadmin:dbadmin@192.168.16.84:5432/fxet')
        self.initialize()
        pass

    def initialize(self):
        self.set_price_streams_info()
        self.set_jobs()
        self.set_queue()

    def push_settings_from_csv_to_postgres(self):
        df_info = pd.read_csv('C:\\Users\\ruagrs7\\scripts\\skewing\\Pictures\\Groups.csv', delimiter=";", header=0)
        df_info = df_info[['Id', 'Name', 'Visible Src Name', 'Market Type', 'Quotes Delay', 'Flags', 'Lot']]
        df_info['Flags'] = df_info['Flags'] == 'Fill Or Kill'
        df_info.columns = ['id', 'name', 'visible_src_name', 'market_type', 'quotes_delay', 'fill_or_kill', 'lot_mio']
        df_info.to_sql('spreads_settings', self.local_engine, if_exists='append', index=False)

    # def get_all_instruments_for_all_groups(self):
    #     query = "SELECT distinct Instrument, PricingGroup from " + SORTED_COLLECTION_PRICES
    #     vertica_cursor.execute(query)
    #     return pd.DataFrame(vertica_cursor.fetchall(), columns=['instrument', 'pricing_group'])

    def get_all_instruments_for_all_groups(self):
        query = "SELECT * FROM instruments_groups"
        postgres_cursor.execute(query)
        return pd.DataFrame(postgres_cursor.fetchall(), columns=['instrument', 'pricing_group'])

    def get_instruments(self, pricing_group):
        return np.array(
            self.df_pricing_group_instruments[self.df_pricing_group_instruments['pricing_group'] == (str(pricing_group) + ":")]['instrument'])

    def set_price_streams_info(self):
        postgres_cursor.execute("SELECT id, name, lot_mio FROM spreads_settings")
        df_info = pd.DataFrame(postgres_cursor.fetchall(), columns=['id', 'name', 'lot_mio'])
        self.df_pricing_group_instruments = self.get_all_instruments_for_all_groups()
        # with open('data.pickle', 'wb') as f:
        #     pickle.dump(self.df_pricing_group_instruments, f)
        # info = np.array(df_info['name'])
        # market_indices = np.where(is_moex)
        # client_indices = np.delete(np.arange(len(df_info)), market_indices)
        # self.PricingGroupsMICEX = np.array(df_info['id'])[market_indices]
        # self.PricingGroupsCLIENT = np.array(df_info['id'])[client_indices]
        self.Locations = ['MSK', 'LND']
        # self.PricingGroups = np.concatenate((self.PricingGroupsMICEX, self.PricingGroupsCLIENT), axis=0)
        for i in range(len(df_info)):
            if 'MOEX' in df_info['name'][i]:
                df_info['name'][i] = 'MOEX'
        self.dict_pricing_group_size = dict(zip(df_info['id'], df_info['lot_mio']))
        self.dict_pricing_group_name = dict(zip(df_info['id'], df_info['name']))
        for i in self.dict_pricing_group_name.keys():
            self.dict_pricing_group_name[i] = "'" + self.dict_pricing_group_name[i] + "'"
        self.PricingGroups = np.array(df_info['id'])
        # self.dict_pricing_group_id = dict(zip(df_info['name'], df_info['id']))

    @staticmethod
    def get_vertica_cursor(params, side):
        # DO not forget to delete limit statement here !!!
        query = "SELECT time, " + side + ", Instrument FROM " + \
                SORTED_COLLECTION_PRICES + " where " \
                                           " Location=:loc and PricingGroup=:PricingGroup and Instrument =:instr limit 100000"
        vertica_cursor.execute(query, params)
        return vertica_cursor

    @staticmethod
    def get_source_by_pricing_group(dict_pricing_group_name, pricing_group):
        return dict_pricing_group_name[pricing_group]

    @staticmethod
    def get_size(dict_pricing_group_size, pricing_group):
        return dict_pricing_group_size[pricing_group]

    @staticmethod
    def is_bid(side):
        if side == 'Bid':
            return "True"
        else:
            return "False"

    def set_jobs(self):
        self.jobs = []
        for side in ['Bid', 'Ask']:
            for location in self.Locations:
                for pricing_group in self.PricingGroups:
                    for instrument in self.get_instruments(pricing_group):
                        self.jobs.append((side, location, instrument, pricing_group))

    def set_queue(self):
        self.queue = mp.JoinableQueue()
        for job in self.jobs:
            self.queue.put(job)
        for i in range(self.nCPU):
            self.queue.put(None)


def transform_instrument(instrument):
    return "'" + instrument[:3] + '/' + instrument[3:] + "'"


def do(session, side, location, instrument, pricing_group, dict_pricing_group_name=None, dict_pricing_group_size=None):
    params = {
        'loc': location,
        'PricingGroup': str(pricing_group) + ":",
        'instr': instrument}
    verticaCursor = VerticaCassandraPricePush.get_vertica_cursor(params, side)
    source = VerticaCassandraPricePush.get_source_by_pricing_group(dict_pricing_group_name, pricing_group)
    size = VerticaCassandraPricePush.get_size(dict_pricing_group_size, pricing_group)
    loc = "'" + location + "'"
    transformed_instrument = transform_instrument(instrument)
    prepared_statement = session.prepare("INSERT INTO increments "
                                         "(source, instrument,location, date_id,timestamp,is_bid,size,price) "
                                         "VALUES (" + source + "," + transformed_instrument + "," + loc + ",?,?," + VerticaCassandraPricePush.is_bid(
        side) + "," + str(
        int(size * 10 ** 6)) + ",?)")
    # i = 0
    for vertica_row in verticaCursor.iterate():
        cassandra_tuple = (str(vertica_row[0].date()),
                           vertica_row[0], vertica_row[1])
        session.execute_async(prepared_statement, cassandra_tuple)
        # i += 1
        # if i % 10000 == 0:
        #     print("Good job: " + str(datetime.now()))


def worker_job(queue, i, dict_pricing_group_name, dict_pricing_group_size):
    # print("Thread # " + str(i) + " start working")
    # while True:
    #     try:
    #         job = queue.get()
    #         print("Thread # " + str(i) + " have finished pushing " + str(job[0]) + ", " + str(job[1]) + ", " + str(job[2]) + ", " + str(job[3]))
    #         queue.task_done()
    #     except Empty:
    #         break
    while True:
        job = queue.get()
        if job is None:
            break
        do(session, job[0], job[1], job[2], job[3], dict_pricing_group_name, dict_pricing_group_size)
        # print("Thread # " + str(i) + " have finished pushing " + str(job[0]) + ", " + str(job[1]) + ", " + str(job[2]) + ", " + str(job[3]))
        queue.task_done()
    queue.task_done()
    # print ("Thread # " + str(i) + " finished work")


if __name__ == '__main__':

    def run(nCPU):
        pusher = VerticaCassandraPricePush(nCPU)
        workers = []
        for i in range(nCPU):
            worker = mp.Process(target=worker_job, args=(pusher.queue, i, pusher.dict_pricing_group_name, pusher.dict_pricing_group_size))
            workers.append(worker)
            worker.start()
        pusher.queue.join()


    run(mp.cpu_count())

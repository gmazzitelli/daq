#!/usr/bin/env python3
#
# G. Mazzitelli 2024
# WC DAQ
#

import TeledyneLeCroyPy
from matplotlib import pyplot as plt
import requests
import numpy as np
import time
import pandas as pd
import h5py
import pickle
import json
import os
import pandas as pd
import gspread
from google.oauth2 import service_account
import json
import subprocess

def append_record_to_hdf5(filename, record_id, record_data):
    with h5py.File(filename, 'a') as hdf_file:
        group = hdf_file.create_group(str(record_id))
        for key, value in record_data.items():
            group.create_dataset(key, data=value)
            
def append_record_to_pickle(filename, record):
    # Read existing data from the pickle file
    try:
        with open(filename, 'rb') as file:
            existing_data = pickle.load(file)
    except FileNotFoundError:
        existing_data = []
    # Append the new record to the existing data
    existing_data.append(record)

    # Write the updated data back to the pickle file
    with open(filename, 'wb') as file:
        pickle.dump(existing_data, file)
        
from pymemcache.client.base import Client


def mc_get_str(ip, key):

    client = Client((ip, 11211))
    result = client.get(key)
    return result.decode("utf-8")


            
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


credentials = service_account.Credentials.from_service_account_file(
    '../.google_credentials.json')
scope = ['https://spreadsheets.google.com/feeds']
creds_scope = credentials.with_scopes(scope)
client = gspread.authorize(creds_scope)

sheet = client.open_by_key('1idBnsYG4pHdHZ2kDbq-S3PBUAf0ewFEzKXOtCTGDxyU')
log = sheet.worksheet("log")

#
# data saved in logbook
# https://docs.google.com/spreadsheets/d/1idBnsYG4pHdHZ2kDbq-S3PBUAf0ewFEzKXOtCTGDxyU
#


start = time.time()
while True:
    try:
        #o = TeledyneLeCroyPy.LeCroyWaveRunner('TCPIP0::192.168.99.103::inst0::INSTR') # CMS
        o = TeledyneLeCroyPy.LeCroyWaveRunner('TCPIP0::192.168.189.115::inst0::INSTR') # BTF
        break
    except:
        end = time.time()
        print("waiting for connection... "+str(int(end-start))+"s", end="\r")
        time.sleep(0.1)
        pass
print("Connected", o.idn) # Prings e.g. LECROY,WAVERUNNER9254M,LCRY4751N40408,9.2.0

# end to init connection

EVENTS= 1000
DSHOW = False
FTYPE = 'PKL'
CHANNELS = 4
PATH = '/Volumes/WC/data/'

while True:
    try:
        input("\nPress Enter to start/continue, Ctr-C to exit")
        # reading logbook
        logdf=pd.DataFrame.from_dict(log.get_all_records())
        last_run = logdf.run.values[-1]
        run = last_run+1
        
        print ("\n-------> DAQ ready to acquire run number: {:05d}".format(run))
        user_input = input("Enter run description? (if any) ")
        if user_input:
            start_desc=user_input
        else:
            start_desc=''

        user_input = input("Number of channels: [{}] ".format(CHANNELS)).lower()
        if user_input:
            channels=int(user_input)
        else:
            channels=CHANNELS
            
        user_input = input("Number of events: [{}] ".format(EVENTS)).lower()
        if user_input:
            events=int(user_input)
        else:
            events=EVENTS
        
#         user_input = input("file type: [{}] ".format(FTYPE)).lower()
#         if user_input:
#             pkl = False
#         else:
#             pkl = True
        
        pkl=False


        if pkl:
            filename = "run_{:05d}.pkl".format(run)
        else:
            filename = "run_{:05d}.h5".format(run)
        filepath = PATH+filename

        #########################
        start = time.time()
        dt = 0

        if os.path.exists(filepath):
            os.remove(filepath)
            print('File removed:', filepath)
        else:
            print('Writing on new file:', filepath)

        beam = mc_get_str(ip='192.168.198.164', key='BTFDATA_PADME')
        # updating startup condition
        start_date = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        start_epoch = time.time()
        logdf = logdf._append({'run': run, 'start_desc': start_desc, 'start_date': start_date, 'start_epoch': start_epoch, 
                               'filename': filename, 'beam': beam}, ignore_index=True)


        for event in range(events):
            dict2save = {}
            triggers = []
            try:
                elapsed = time.time()-start
                if event >0:
                    trate = elapsed/event
                else:
                    trate = 0 
                print('Triggers acquired: {:d}, elapsed {:.1f} s, Tr Hz: {:.1f}, storing time: {:.2f} s'.format(event,elapsed, trate, dt), end="\r")
                o.wait_for_single_trigger() # Halt the execution until there is a trigger.
                dt1 = time.time()
                for channel in range(1, channels+1):
                    data = o.get_waveform(n_channel=channel)
                    if pkl:
                        triggers.append(data)
                    else:
                        dict2save['H'+str(channel)]=json.dumps(data['wavedesc'], default=str)
                        dict2save['T'+str(channel)]=json.dumps(data['trigtime'])
                        dict2save['W'+str(channel)]=json.dumps(data['waveforms'], cls=NumpyEncoder)        
                    if DSHOW:
                        # show first waveform per 
                        x = data['waveforms'][0]['Time (s)']/1e-9
                        y = data['waveforms'][0]['Amplitude (V)']/1e-3
                        plt.plot(x,y, label="C"+str(n_channel))


                if pkl:
                    append_record_to_pickle(filepath, triggers)
                else:
                    dict2save['epoch']=data['wavedesc']['TRIGGER_TIME'].timestamp()
                    append_record_to_hdf5(filepath, event, dict2save)
                if DSHOW:
                    t = data['wavedesc']['TRIGGER_TIME'].strftime('%m/%d/%Y %H:%M:%S')
                    plt.legend()
                    plt.title(t)
                    plt.xlabel('ns')
                    plt.ylabel('mV')
                    plt.show()
                dt =  time.time() - dt1
            except KeyboardInterrupt:
                print ("\nstopping DAQ...")
                break
        print ("\n-------> DAQ acquisition of RUN {:05d} completed".format(run))
        print ("Upating metadata run info...")
        # updating ending condition
        end_date = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        logdf.at[run, 'end_date']=end_date
        end_epoch = time.time()
        logdf.at[run, 'end_epoch']=end_epoch
        logdf.at[run, 'events']=event+1

        print ("Uploding file on cloud in background...")
        subprocess.Popen(['./uploadFile.py', '-g', '-r', filepath], stdout=None, stderr=None, stdin=None)

        user_input = input("Enter closing remarks? (if any) ")
        if user_input:
            end_desc=user_input
        else:
            end_desc=''
        logdf.at[run, 'end_desc']=end_desc

        # updating logbook
        print ("Updating logbook...")
        log.update([logdf.columns.values.tolist()] + logdf.values.tolist())

        print("\n-------> RUN {:} ACQUIRED".format(filename))

    except KeyboardInterrupt:
        break
print("\nDAQ STOP")
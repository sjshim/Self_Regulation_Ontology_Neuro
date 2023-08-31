import glob
import pandas as pd
import numpy as np

old_path = '/oak/stanford/groups/russpold/data/uh2/aim1/behavioral_data/event_files/'
new_path = '/oak/stanford/groups/russpold/data/uh2/aim1/behavioral_data/event_files_sharing/'

old_events = sorted(glob.glob(old_path + '*'))
new_events = sorted(glob.glob(new_path + '*'))

# for old_file, new_file in zip(old_events, new_events):
#     old_df = pd.read_csv(old_file, sep='\t')
#     new_df = pd.read_csv(new_file, sep='\t')
    # assert old_df['onset'].equals(new_df['onset'])
    # assert old_df['duration'].equals(new_df['duration'])
    # assert old_df['response_time'].equals(new_df['response_time'])
    # assert old_df.equals(new_df), "DataFrames are not equal."

for file in new_events:
    df = pd.read_csv(file, sep='\t')
    if df['worker_id'].any() == np.nan:
        print(file)
import argparse
from collections import defaultdict
from clean_raw_behavior import clean_data
from glob import glob
import os
import pandas as pd
from create_event_utils import create_events
# some DVs are defined in utils if they deviate from normal expanalysis
from utils import get_name_map, get_timing_correction, get_median_rts, get_mean_rts, get_neg_rt_correction, fix_swapped_keys
#for working in jupyter lab 

# parser = argparse.ArgumentParser()
# parser.add_argument('--clear', action='store_true')
# parser.add_argument('--quiet', action='store_false')
# args = parser.parse_args()
# clear = args.clear
# verbose = args.quiet
# aims=args.aim

aims = ['aim1']
clear = True
verbose = True
no_workerid = []
for aim in aims:
    print('beginning %s' % aim)
    # set up map between file names and names of tasks
    name_map = get_name_map()

    #task_dfs = defaultdict(pd.DataFrame)

    #make directories

    # clean data
    if verbose: print("Processing Tasks")
    raw_files=[]
    if aim=='aim1':
        raw_files = sorted(glob('/oak/stanford/groups/russpold/data/uh2/aim1/raw_behavioral_data/raw/*/*'))
        
    for subj_file in raw_files:
        filey = os.path.basename(subj_file)
        cleaned_file_name = '_cleaned.'.join(filey.split('.'))
        event_file_name = '_events.'.join(filey.split('.')).replace('csv','tsv')
        os.makedirs(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/processed_sharing/', exist_ok = True)
        os.makedirs(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/event_files_sharing/', exist_ok = True)
        cleaned_file_path = os.path.join(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/processed_sharing/%s' % cleaned_file_name)
        events_file_path = os.path.join(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/event_files_sharing/%s' % event_file_name)

        # if this file has already been cleaned, continue
        # if os.path.exists(cleaned_file_path):
        #     df = pd.read_csv(cleaned_file_path)
        #     exp_id = df.experiment_exp_id.unique()[0] #gets the value of experiment_exp_id, and assigns it to exp_id
        # else:
            # else proceed
        df = pd.read_csv(subj_file, engine='python')
        # get exp_id
        if 'exp_id' in df.columns:
            exp_id = df.iloc[-2].exp_id 
        else:
            exp_id = '_'.join(os.path.basename(subj_file).split('_')[1:]).rstrip('.csv')
            
        #fix typo
        if '__fmri' in exp_id:
            exp_id = exp_id.replace('__fmri', '')

        #fixes difference in rest scanner input 
        if (exp_id == 'rest'): 
            df = df.replace(to_replace='scanner_wait', value = 'fmri_trigger_wait', regex=True)
        # set time_elapsed in reference to the last trigger of internal calibration
        else:
            start_time = df.query('trial_id == "fmri_trigger_wait"').iloc[-1]['time_elapsed'] 
            df.time_elapsed-=start_time 
            
            # correct start time for problematic scans
            df.time_elapsed-=get_timing_correction(filey)
            df = get_neg_rt_correction(filey, df)
            df = fix_swapped_keys(filey, df)
            # correct negative RTs
            # make sure the file name matches the actual experiment
            assert name_map[exp_id] in subj_file, \
                print(name_map[exp_id]+'file %s does not match exp_id: %s' % (subj_file, exp_id))
            if exp_id == 'columbia_card_task_hot':
                exp_id = 'columbia_card_task_fmri'
            df.loc[:,'experiment_exp_id'] = exp_id
            # make sure there is a subject column
            if 'subject' not in df.columns:
                no_workerid.append(filey)
            df['worker_id'] = filey.split('_')[0]
            # change column from subject to worker_id
            #df.rename(columns={'subject':'worker_id'}, inplace=True)

            # post process data, drop rows, etc.....
            drop_columns = ['view_history', 'stimulus', 'trial_index',
                            'internal_node_id', 'test_start_block','exp_id',
                            'trigger_times']
            
            df = clean_data(df, exp_id=exp_id, drop_columns=drop_columns)
            # drop unnecessary rows
            drop_dict = {'trial_type': ['text'], 'trial_id': ['fmri_response_test', 'fmri_scanner_wait',
                                    'fmri_trigger_wait', 'fmri_buffer', 'scanner_wait', 'scanner_rest', 
                                    'end']}
            for row, vals in drop_dict.items():
                df = df.query('%s not in  %s' % (row, vals))
            df.to_csv(cleaned_file_path, index=False)
        #task_dfs[exp_id] = pd.concat([task_dfs[exp_id], df], axis=0)

if verbose: print("Creating Event Files")
# calculate event files
for subj_file in raw_files:
    filey = os.path.basename(subj_file)
    cleaned_file_name = '_cleaned.'.join(filey.split('.'))
    event_file_name = '_events.'.join(filey.split('.')).replace('csv','tsv')
    cleaned_file_path = os.path.join(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/processed_sharing/%s' % cleaned_file_name)
    events_file_path = os.path.join(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/event_files_sharing/%s' % event_file_name)
    os.makedirs(f'/oak/stanford/groups/russpold/data/uh2/{aim}/behavioral_data/event_files', exist_ok=True) 

  # get & save cleaned file
    if 'preRating' not in cleaned_file_path:
        df = pd.read_csv(cleaned_file_path)
        exp_id = df.experiment_exp_id.unique()[0]
        if exp_id == 'manipulation_task':
            preRating_file = subj_file.replace('manipulationTask', 'preRating')
            if os.path.isfile(preRating_file):
                preRating_df = pd.read_csv(preRating_file)
                events_df = create_events(df, exp_id, aim+'/behavioral_data', duration=None, preRating_df = preRating_df)
            else:
                print(f'File does not exist: {preRating_file}')
                events_df = create_events(df, exp_id, aim+'/behavioral_data', duration=None, preRating_df = None)

        events_df = create_events(df, exp_id, aim+'/behavioral_data', duration=None)
        if events_df is not None:
            # Move 'onset' and 'duration' columns to the front
            cols = ['onset', 'duration'] + [col for col in events_df if col not in ['onset', 'duration']]
            events_df = events_df[cols]
            events_df = events_df.fillna('n/a')
            events_df.to_csv(events_file_path, sep='\t', index=False)
        else:
            print("Events file wasn't created for %s" % subj_file)

if verbose: print("Finished Processing")



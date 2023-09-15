import numpy as np
import pandas as pd
from utils import get_survey_items_order
# *********************************
# helper functions
# *********************************
def get_drop_columns(df, columns=None, use_default=True):
    """
    defines columns to drop when converting from _clean to _event
    files each event file. Generates a list of columns to drop,
    constrains it to columns already in the dataframe.
    """
    default_cols = ['exp_stage',
                    'feedback_duration', 'possible_responses',
                    'stim_duration', 'text', 'time_elapsed',
                   'timing_post_trial', 'trial_num']
    drop_columns = []
    if columns is not None:
        drop_columns = columns
    if use_default == True:
        #if true, drop columns come from argument and from default (inclusive or)
        drop_columns = set(default_cols) | set(drop_columns)
     #drop columns only included if they appear in the dataframe   
    drop_columns = set(df.columns) & set(drop_columns)
    return drop_columns

def get_movement_times(df):
    """
    time elapsed is evaluated at the end of a trial, so we have to subtract
    timing post trial and the entire block duration to get the time when
    the trial started. Then add the reaction time to get the time of movement
    """
    trial_time = df.time_elapsed - df.block_duration + \
                 df.rt
    return trial_time
    
def get_trial_times(df):
    """
    time elapsed is evaluated at the end of a trial, so we have to subtract
    timing post trial and the entire block duration to get the time when
    the trial started
    """
    trial_time = df.time_elapsed - df.block_duration
    return trial_time

def process_rt(events_df):
    """changes -1 rts (javascript no response) to nan, changes column from rt -> response_time """
    events_df.loc[events_df['rt'] == -1, 'rt'] = np.nan #replaces no response rts with nan
    events_df = events_df.rename(columns={'rt': 'response_time'})
    return events_df

def row_match(df,row_list):
    bool_list = pd.Series(True,index=df.index)
    for i in range(len(row_list)):
        bool_list = bool_list & (df.iloc[:,i] == row_list[i])
    return bool_list[bool_list].index    


def create_events(df, exp_id, aim, duration=None, preRating_df = None):
    """
    defines what function to reference to create each task-specific event file 
    takes in a dataframe from processed data, and exp_id and a duration
    """ 
    events_df = None
    lookup = {'attention_network_task': create_ANT_event,
              'columbia_card_task_fmri': create_CCT_event,
              'discount_fixed': create_discountFix_event,
              'dot_pattern_expectancy': create_DPX_event,
              'motor_selective_stop_signal': create_motorSelectiveStop_event,
              'stop_signal': create_stopSignal_event,
              'stroop': create_stroop_event,
              'survey_medley': create_survey_event,
              'twobytwo': create_twobytwo_event,
              'ward_and_allport': create_WATT_event}
    fun = lookup.get(exp_id)
    if fun is not None:
        if exp_id != 'columbia_card_task_fmri':
            events_df = fun(df, aim, duration=duration)
        elif exp_id == 'manipulation_task':
            events_df = fun(df, aim, preRating_df = preRating_df)
        else:
            events_df = fun(df, aim)
    return events_df

# *********************************
# Functions to create event files
# *********************************

def create_ANT_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns = ['trial_type', 'block_duration', 'trial_id'])
    events_df = df[df['time_elapsed']>0].copy()
    
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)

    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)

    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)

    return events_df

def create_CCT_event(df, aim):
    columns_to_drop = get_drop_columns(df, columns = ['cards_left',
                                                    'round_points',
                                                    'which_round',
                                                    'trial_type',
                                                    'block_duration'])

    events_df = df[df['time_elapsed']>0].copy()
    events_df.insert(0, 'duration', events_df.rt)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # change onset of ITI columns to reflect the fact that the stimulus changes 750 ms after block starts
    # ITI_trials = events_df.query('trial_id == "ITI"').index

    # events_df.loc[ITI_trials, 'duration'] = events_df.loc[ITI_trials, 'stim_duration']-750
    # print(events_df.loc[ITI_trials])
    all_frames = []  # List to store chunks of DataFrame
    start_idx = 0

    for index, row in events_df.iterrows():
        if row['trial_id'] == 'ITI':
            all_frames.append(events_df.loc[start_idx:index-1])

            first_row = row.copy()
            second_row = row.copy()

            first_row['duration'] = row['stim_duration']
            first_row['trial_id'] = 'feedback'
            second_row['duration'] = row['block_duration'] - row['stim_duration']
            second_row['trial_id'] = 'ITI'
            second_row['onset'] = row['onset'] + row['stim_duration']
            
            all_frames.extend([pd.DataFrame([first_row]), pd.DataFrame([second_row])])
            
            start_idx = index + 1

    all_frames.append(events_df.loc[start_idx:])
    df_split = pd.concat(all_frames, ignore_index=True)

    events_df = df_split
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration']]/=1000
    # # add feedback columns
    # events_df.loc[:,'feedback'] = events_df.clicked_on_loss_card \
    #                                 .apply(lambda x: int(not x))
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)

    return events_df

def create_discountFix_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns = ['trial_id', 'block_duration'])
    events_df = df[df['time_elapsed']>0].copy()

    events_df.loc[:,'trial_type'] = events_df.choice
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration']]/=1000

    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)

    return events_df

def create_DPX_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns=['block_duration'])
    events_df = df[df['time_elapsed']>0].copy()

    events_df.loc[:,'trial_type'] = events_df.condition
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration. We also want the trial
    onsets = get_trial_times(df)
    events_df.insert(0,'onset',onsets)
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset',
                     'duration']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
        
    return events_df

def create_motorSelectiveStop_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns = ['condition',
                                                    'SS_duration',
                                                    'SS_stimulus',
                                                    'SS_trial_type',
                                                    'block_duration',
                                                    'correct',
                                                    'trial_id'])
    events_df = df[df['time_elapsed']>0].copy()

    # create condition column
    crit_key = events_df.query('condition=="stop"') \
                .correct_response.unique()[0]
    noncrit_key = events_df.query('condition=="ignore"') \
                    .correct_response.unique()[0]
    condition_df = events_df.loc[:,['correct_response',
                                    'SS_trial_type','stopped']]
    condition = pd.Series(index=events_df.index)
    condition[row_match(condition_df, [crit_key,'go',False])] = 'crit_go'
    condition[row_match(condition_df, [crit_key,'go',True])] = 'crit_go'
    condition[row_match(condition_df,
                        [crit_key,'stop',True])] = 'crit_stop_success'
    condition[row_match(condition_df,
                        [crit_key,'stop',False])] = 'crit_stop_failure'
    condition[row_match(condition_df,
                        [noncrit_key,'stop',False])] = 'noncrit_signal'
    condition[row_match(condition_df,
                        [noncrit_key,'go',False])] = 'noncrit_nosignal'
    condition[row_match(condition_df,
                        [noncrit_key,'stop',True])] = 'noncrit_signal'
    condition[row_match(condition_df,
                        [noncrit_key,'go',True])] = 'noncrit_nosignal'
    
    events_df.loc[:,'trial_type'] = condition
    
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration', 'SS_delay']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
        
    return events_df

def create_stopSignal_event(df, aim, duration=None):

    events_df = df[df['time_elapsed']>0].copy()

    # create condition label
    SS_success_trials = events_df.query('SS_trial_type == "stop" \
                                        and stopped == True').index
    SS_fail_trials = events_df.query('SS_trial_type == "stop" \
                                        and stopped == False').index
    events_df.loc[:,'condition'] = 'go'
    events_df.loc[SS_success_trials,'condition'] = 'stop_success'
    events_df.loc[SS_fail_trials,'condition'] = 'stop_failure'
    events_df.loc[:,'trial_type'] = events_df.condition
    
    events_df.loc[events_df['trial_type'] == 'stop_success', ['correct']] = 1
    # duration    
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block

    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration', 'block_duration', 'SS_delay']]/=1000
    # get and drop unnecessary columns
    columns_to_drop = get_drop_columns(events_df, columns = ['condition',
                                                    'SS_duration',
                                                    'SS_stimulus',
                                                    'SS_trial_type',
                                                    'block_duration',
                                                    'correct',
                                                    'trial_id'])    
    events_df = events_df.drop(columns_to_drop, axis=1)

    return events_df

def create_stroop_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns = ['block_duration',
                                                    'trial_id'])
    events_df = df[df['time_elapsed']>0].copy()

    events_df.loc[:,'trial_type'] = events_df.condition
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration','block_duration']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
        
    return events_df

def create_survey_event(df, aim, duration=None):
    columns_to_drop = get_drop_columns(df,
                                       use_default=False,
                                       columns = ['block_duration',
                                                'response',
                                                'options',
                                                'stim_duration',
                                                'text',
                                                'time_elapsed',
                                                'timing_post_trial',
                                                'trial_id',
                                                'item_responses'
                                                ])
    events_df = df[df['time_elapsed']>0].copy()
    # add signifiers for each question
    events_df['trial_type'] = df['item_text'].map(get_survey_items_order())
    # add duration and response regressor
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:#
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset','duration']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
        
    return events_df

def create_twobytwo_event(df,aim, duration=None):
    columns_to_drop = get_drop_columns(df, columns=['block_duration',
                                                    'trial_id',
                                                    'trial_type'])
    events_df = df[df['time_elapsed']>0].copy()
    block_start_indexes = events_df.index[events_df['trial_id'] == 'test_start_block'].tolist()
    events_df['first_trial_of_block'] = 0
    for n in range(len(block_start_indexes)):
        ind = block_start_indexes[n]
        events_df.at[ind+1, 'first_trial_of_block'] = 1
    events_df = events_df[events_df['trial_id'] != 'test_start_block']
    # reorganize and rename columns in line with BIDs specifications
    if duration is None:
        events_df.insert(0,'duration',events_df.stim_duration)
    else:
        events_df.insert(0,'duration',duration)
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['response_time','onset',
                     'duration','CTI', 'block_duration']]/=1000
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
    
    #change color to blue
    events_df = events_df.replace('#1F45FC', 'blue')

    return events_df

def create_WATT_event(df,aim, duration):
    columns_to_drop = get_drop_columns(df, columns = ['correct',
                                                      'min_moves',
                                                      'num_moves_made',
                                                      'problem_time',
                                                      'trial_type',
                                                      'block_duration'])
    columns_to_drop.remove('exp_stage')
    events_df = df.copy()
    # get planning, movement, and feedback index
    practice_planning = events_df.query('exp_stage == "practice" \
                                    and trial_id == "to_hand" \
                                    and num_moves_made==1').index
    practice_other = events_df.query('exp_stage == "practice" \
                                    and not(trial_id == "to_hand" \
                                    and num_moves_made==1) \
                                    and trial_id != "feedback"').index
    planning_moves = events_df.query('exp_stage == "test" \
                                    and trial_id == "to_hand" \
                                    and num_moves_made==1').index
    other_moves = events_df.query('exp_stage == "test" \
                                    and not (trial_id == "to_hand" \
                                    and num_moves_made==1) \
                                    and trial_id != "feedback"').index
    feedback = events_df.query('trial_id == "feedback"').index
    # add planning indicator
    events_df.insert(1,'planning',0)
    events_df.loc[planning_moves,'planning'] = 1
    events_df.loc[practice_planning,'planning'] = 1
    # ** Onsets **
    # time elapsed is at the end of the trial, so have to remove the block
    # duration
    events_df.insert(0,'onset',get_trial_times(df))
    events_df.insert(0, 'duration', 0)
    
    # add durations for planning
    planning_total = events_df[(events_df.trial_id=='to_hand') & (events_df.num_moves_made == 1)].index.values
    events_df.loc[planning_total, 'duration'] = events_df.loc[planning_total, 'rt']
    events_df.loc[practice_other, 'duration'] = events_df.loc[practice_other, 'rt']
    events_df.loc[other_moves, 'duration'] = events_df.loc[other_moves, 'rt']

    # add durations for feedback
    events_df.loc[feedback, 'duration'] = events_df.loc[feedback, 'stim_duration']

    subject = events_df['worker_id'].unique()[0]

    # Identify the feedback rows
    feedback_rows = events_df[events_df['trial_id'] == 'feedback'].copy()
    # Calculate the duration for the 'ITI' rows
    feedback_rows['ITI_duration'] = feedback_rows['block_duration'] - feedback_rows['duration']
    # Calculate the onset for the 'ITI' rows
    feedback_rows['ITI_onset'] = feedback_rows['onset'] + 1000
    # Create the 'ITI' rows by copying the preceding rows
    iti_rows = feedback_rows.copy()

    # Shift the values down one row to match the preceding trial
    iti_rows = iti_rows.shift(periods=-1, axis=0)

    # Modify the columns that should be different for 'ITI'
    iti_rows['duration'] = iti_rows['ITI_duration']
    iti_rows['onset'] = iti_rows['ITI_onset']
    iti_rows['trial_id'] = 'ITI'
    iti_rows['planning'] = 0  # or whatever value is appropriate for 'ITI'

    # Here, the rest of the columns will inherit values from the preceding row (trial)
    # If you want to manually override some columns, you can do so here. For example:
    # iti_rows['condition'] = 'ITI_condition'

    # Remove the extra ITI_duration and ITI_onset columns
    iti_rows = iti_rows.drop(['ITI_duration', 'ITI_onset'], axis=1)
    # Append the 'ITI' rows to the original dataframe
    events_df = pd.concat([events_df, iti_rows])

    # Sort the dataframe based on the onset column
    events_df = events_df.sort_values(by='onset').reset_index(drop=True)

    # process RT
    events_df = process_rt(events_df)
    # convert milliseconds to seconds
    events_df.loc[:,['onset','duration',
                     'response_time']]/=1000
    
    # drop unnecessary columns
    events_df = events_df.drop(columns_to_drop, axis=1)
    
    # fix typo
    events_df['condition'] = events_df['condition'].str.replace('intermeidate', 'intermediate')

    return events_df

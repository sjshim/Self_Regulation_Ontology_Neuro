from glob import glob
from os import path
import pandas as pd
import numpy as np
#for WATT min_moves calculator
import copy 
#import msgpack

# function to correct processing of a few problematic files
# need to change time_elapsed to reflect the fact that fmri triggers were
# sent out too quickly (at 8 times the rate), thus starting the scan 14 TRs
# early. Those 14 TRs of data therefore need to be thrown out, which is
# accomplished by setting the "0" of the scan 14 TRs later

# scratch the above, the duration of the trigger_wait trials were about 85ms 
# instead of 680ms as with other tasks, for about 15 trials
# so subtracting tr_correction amount from all trials' time_elapsed
def aim4_latency_correction(filey, df):
    problematic_subjects = {'be4372': ['ses-02'], 
                            'be4879': ['ses-01', 'ses-02'],
                            'be4631': ['ses-01', 'ses-02'],
                            'be5036': ['ses-01', 'ses-02'],
                            'be5130': ['ses-01', 'ses-02'],
                            'be4306': ['ses-01'], 
                            's1378': ['ses-01']}
    subject_info = filey.split('_')
    sub = subject_info[0]
    session =  subject_info[1]
    task = subject_info[2].split('.')[0]
    
    if sub in problematic_subjects.keys():
        print(sub, session)
        if session in problematic_subjects[sub]:
                index = df[df['trial_id'] == 'fmri_trigger_wait'].index[0]
                df.time_elapsed[index+1:] += 133
    return df
    

def get_timing_correction(filey, TR=680, n_TRs=14):
    problematic_files = ['s568_motorSelectiveStop.csv', 's568_stroop.csv',
                         's568_surveyMedley.csv', 's568_DPX.csv',
                         's568_discountFix.csv',
                         's556_motorSelectiveStop.csv', 's556_stroop.csv',
                         's556_surveyMedley.csv', 's556_DPX.csv',
                         's556_discountFix.csv',
                         's561_WATT3.csv', 's561_ANT.csv',
                         's561_twoByTwo.csv', 's561_CCTHot.csv',
                         's561_stopSignal.csv']
    #tr_correction = TR * n_TRs
    tr_correction = (680-85)*15
    if filey in problematic_files:
        return tr_correction
    else:
        return 0

def get_neg_rt_correction(filey, df):
    problematic_files = ['s608_ANT.csv']
    if filey in problematic_files:
            i = df.loc[df.rt < -1].index.values.astype(int)[0]
            trial_before = df.iloc[i-1]['time_elapsed']
            problematic = df.iloc[i:]
            block_durations= problematic.block_duration.to_list()
            new_time_elapsed = []
            for n in range(len(block_durations)):
                new_time_elapsed.append(trial_before+block_durations[n])
                trial_before = trial_before+block_durations[n]
            new_time = df.iloc[:i].time_elapsed.to_list()+new_time_elapsed
            df['time_elapsed'] = new_time
            return df
    else:
        return df

def fix_swapped_keys(filey, df):
    problematic_files = ['s644_stroop.csv']
    if filey in problematic_files:
        df.loc[df['key_press'] == 71, 'key_press'] = 43
        df.loc[df['key_press'] == 82, 'key_press'] = 71
        df.loc[df['key_press'] == 43, 'key_press'] = 82
        correct = (df['key_press'] == df['correct_response'])
        df['correct'][correct] = 1
        incorrect = (df['key_press'] != df['correct_response'])
        df['correct'][incorrect] = 0
    return df
    
def get_name_map():
    name_map = {'attention_network_task': 'ANT',
            'columbia_card_task_hot': 'CCTHot',
            'discount_fixed': 'discountFix',
            'dot_pattern_expectancy': 'DPX',
            'motor_selective_stop_signal': 'motorSelectiveStop',
            'stop_signal': 'stopSignal',
            'stroop': 'stroop',
            'survey_medley': 'surveyMedley',
            'twobytwo': 'twoByTwo',
            'ward_and_allport': 'WATT3',
            'manipulation_task': 'manipulationTask',
            'pre_rating': 'preRating',
            'rest': 'rest',
            'uh2_video': 'rest',    
                #for the manipulation tasks that have 'cue_control_food' for the exp_id
            'cue_control_food': 'manipulationTask'}
    
    return name_map  


def get_event_files(subj):
    file_dir = path.dirname(__file__)
    event_files = {}
    for subj_file in glob(path.join(file_dir, '../behavioral_data/event_files/*%s*' % subj)):
        df = pd.read_csv(subj_file, sep='\t')
        exp_id = path.basename(subj_file).split('_')[1]
        event_files[exp_id] = df
    return event_files

def get_processed_files(subj):
    file_dir = path.dirname(__file__)
    processed_files = {}
    for subj_file in glob(path.join(file_dir, '../behavioral_data/processed/*%s*' % subj)):
        df = pd.read_csv(subj_file)
        exp_id = path.basename(subj_file).split('_')[1]
        processed_files[exp_id] = df
    return processed_files

def participant_means(df):
    return [np.mean(df.rt[(df.worker_id==subj) & (df.rt>0)]) for subj in df.worker_id.unique()]

def get_mean_rts(task_dfs):
    """function that calculates median RT"""
    task_mean_rts = {task: participant_means(df) for task,df in task_dfs.items()}
#     # special cases handled below
#     # ** twoByTwo **
#     if (len(task_dfs["twobytwo"])>0):
#         print("two by two loop working")
#         median_cue_length = task_dfs['twobytwo'].CTI.quantile(.5)
#         task_50th_rts['twobytwo'] += median_cue_length
#     if (len(task_dfs["ward_and_allport"])>0):
#     # ** WATT3 **
#         WATT_df = task_dfs['ward_and_allport'].query('exp_stage == "test"')
#     # get the first move times (plan times)
#         plan_times = WATT_df.query('trial_id == "to_hand" and num_moves_made==1').rt
#     # get other move times
#         move_times = WATT_df.query('not (trial_id == "to_hand" and num_moves_made==1)')
#     # drop feedback
#         move_times = move_times.query('trial_id != "feedback"').rt
#         task_50th_rts['ward_and_allport'] = {'planning_time': plan_times.quantile(.5),
#                                          'move_time': move_times.quantile(.5)}
    return task_mean_rts

def get_median_rts(task_dfs):
    """function that calculates median RT"""
    task_50th_rts = {task: df.rt[df.rt>0].quantile(.5) for task,df in task_dfs.items()}
    # special cases handled below
    # ** twoByTwo **
    #if (len(task_dfs["twobytwo"])>0):
    #    print("two by two loop working")
    #    median_cue_length = task_dfs['twobytwo'].CTI.quantile(.5)
    #    task_50th_rts['twobytwo'] += median_cue_length
    if (len(task_dfs["ward_and_allport"])>0):
        print('watt happening')
    # ** WATT3 **
        WATT_df = task_dfs['ward_and_allport'].query('exp_stage == "test"')
    # get the first move times (plan times)
        plan_times = WATT_df.query('trial_id == "to_hand" and num_moves_made==1').rt
    # get other move times
        move_times = WATT_df.query('not (trial_id == "to_hand" and num_moves_made==1)')
    # drop feedback
        move_times = move_times.query('trial_id != "feedback"').rt
        task_50th_rts['ward_and_allport'] = {'planning_time': plan_times.quantile(.5),
                                         'move_time': move_times.quantile(.5)}
    return task_50th_rts

def get_survey_items_order():

    """Function which returns dictionary with ordering id (Q01-Q40) assigned to each question.
    This dictionary can be further used to map all quesion to their unique (template) order, therefore, to obtain the same order of beta vales for each person
    Author: Karolina Finc
    """

    grit_items = [
        'New ideas and projects sometimes distract me from previous ones.',
        'Setbacks don\'t discourage me.',
        'I have been obsessed with a certain idea or project for a short time but later lost interest.',
        'I am a hard worker.',
        'I often set a goal but later choose to pursue a different one.',
        'I have difficulty maintaining my focus on projects that take more than a few months to complete.',
        'I finish whatever I begin.',
        'I am diligent.'
    ]

    brief_items = [
        'I am good at resisting temptation.',
        'I have a hard time breaking bad habits.',
        'I am lazy.',
        'I say inappropriate things.',
        'I do certain things that are bad for me, if they are fun.',
        'I refuse things that are bad for me.',
        'I wish I had more self-discipline.',
        'People would say that I have iron self-discipline.',
        'Pleasure and fun sometimes keep me from getting work done.',
        'I have trouble concentrating.',
        'I am able to work effectively toward long-term goals.',
        'Sometimes I can\'t stop myself from doing something, even if I know it is wrong.',
        'I often act without thinking through all the alternatives.'
     ]

    future_time_items = [
        'Many opportunities await me in the future.',
        'I expect that I will set many new goals in the future.',
        'My future is filled with possibilities.',
        'Most of my life lies ahead of me.',
        'My future seems infinite to me.',
        'I could do anything I want in the future.',
        'There is plenty of time left in my life to make new plans.',
        'I have the sense that time is running out.',
        'There are only limited possibilities in my future.',
        'As I get older, I begin to experience time as limited.'
     ]

    upps_items = [
        "Sometimes when I feel bad, I can't seem to stop what I am doing even though it is making me feel worse.",
        'Others would say I make bad choices when I am extremely happy about something.',
        'When I get really happy about something, I tend to do things that can have bad consequences.',
        'When overjoyed, I feel like I cant stop myself from going overboard.',
        'When I am really excited, I tend not to think of the consequences of my actions.',
        'I tend to act without thinking when I am really excited.'
    ]

    impulse_venture_items = [
        'Do you welcome new and exciting experiences and sensations even if they are a little frightening and unconventional?',
        'Do you sometimes like doing things that are a bit frightening?',
        'Would you enjoy the sensation of skiing very fast down a high mountain slope?'
    ]

    item_text = grit_items + brief_items + future_time_items + upps_items + impulse_venture_items
    item_id = ['Q%s' % str(i+1).zfill(2) for i in range(len(item_text))]
    item_id_map = dict(zip(item_text, item_id))

    return item_id_map


# FUNCTIONS TO CALCULATE THE MINIMUM # OF WATT MOVES
def grab_block(state, idx, goal, hand, num_moves, visited_states):
    block_idx = np.max(np.nonzero(state[idx]))
    hand = [state[idx][block_idx]] #put top block in hand
    state[idx][block_idx] = 0 #change block's place to empty
    if state in visited_states: #if this move doesn't progress, skip it
        return np.inf
    else:
        visited_states.append(state)
        return solve_WATT(state, goal, hand, num_moves, visited_states)
    
def place_block(state, idx, goal, hand, num_moves, visited_states):
    #find topmost empty spot on the rod
    block_locs = np.array(np.nonzero(state[idx]))
    if block_locs.size==0:
        block_idx = 0
    else:
        block_idx = np.max(block_locs) + 1
        
    state[idx][block_idx] = hand[0] #place block from hand onto rod
    hand = [] #empty hand
    if state in visited_states: #if this move doesn't progress, skip it
        return np.inf
    else:
        visited_states.append(state)
        num_moves += 1 #update the number of moves
        if num_moves > 16: #if the algo has gone too deeply down a rabbit hole, abort
            return np.inf
        else:
            return solve_WATT(state, goal, hand, num_moves, visited_states)


def solve_WATT(state, goal, hand, num_moves, visited_states):
    if state==goal:
        return num_moves
    else:
        if len(hand)==0:
            return np.nanmin([grab_block(copy.deepcopy(state), idx, goal, copy.deepcopy(hand), num_moves, msgpack.unpackb(msgpack.packb(visited_states))) for idx in range(len(state)) if np.array(np.nonzero(state[idx])).size!=0]) #grab blocks from all possible columns that aren't empty
        elif len(hand)==1:
            return np.nanmin([place_block(copy.deepcopy(state), idx, goal, copy.deepcopy(hand), num_moves, msgpack.unpackb(msgpack.packb(visited_states))) for idx in range(len(state)) if np.array(np.nonzero(state[idx])).size!=np.array(state[idx]).size]) #place block on all columns that aren't full
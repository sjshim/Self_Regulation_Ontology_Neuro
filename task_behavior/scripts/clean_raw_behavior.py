"""
analysis/processing.py: part of expfactory package
functions for automatically cleaning and manipulating experiments by operating
on an expanalysis Result.data dataframe
"""
import pandas
import numpy

def drop_null_cols(df):
    null_cols = df.columns[pandas.isnull(df).sum()==len(df)]     
    df.drop(null_cols,axis = 1, inplace = True)
    
def lookup_val(val):
    """function that modifies a string so that it conforms to expfactory analysis by 
    replacing it with an interpretable synonym
    :val: val to lookup
    """
    try:
        is_str = isinstance(val,(str,unicode))
    except NameError:
        is_str = isinstance(val,str)
    
    if is_str:
        #convert unicode to str
        try:
            if isinstance(val, unicode):
                val = unicodedata.normalize('NFKD', val).encode('ascii', 'ignore')
        except NameError:
            pass
        lookup_val = val.strip().lower()
        lookup_val = val.replace(" ", "_")
        #define synonyms
        lookup = {
        'reaction time': 'rt',
        'instructions': 'instruction',
        'correct': 1,
        'incorrect': 0}
        return lookup.get(lookup_val,val)
    else:
        return val
        
#***********************************
# POST PROCESSING BY TASK
#***********************************
def ANT_post(df):
    df.loc[:,'correct'] = df['correct'].astype(float)
    return df

def CCT_fmri_post(df):
    df['clicked_on_loss_card'] = df['clicked_on_loss_card'].astype(float)
    df['action'] = df.key_press.replace({89:'draw_card',71:'end_round'})
    total_cards = df.loc[df.query('trial_id == "ITI"').index-1].num_click_in_round
    df.loc[df.query('trial_id == "ITI"').index-1,'total_cards'] = total_cards
    # add a click to each end round
    df.loc[df.loc[:,'action'] == "end_round", "num_click_in_round"]+=1
    # add additional variables
    df.loc[:,'cards_left'] = df.num_cards-(df.num_click_in_round-1)
    df.loc[:,'loss_probability'] = df.num_loss_cards/df.cards_left
    df.loc[:,'gain_probability'] = 1-df.loss_probability
    # compute expected value
    EV = df.gain_amount*df.gain_probability \
         + df.loss_amount*df.loss_probability
    df.loc[:,'EV'] = EV-EV.mean()
    # compute risk of each action
    risk = (df.gain_probability * (df.gain_amount-df.EV)**2 \
           + df.loss_probability * (df.loss_amount-df.EV)**2)**.5
    df.loc[:,'risk'] = risk-risk.mean() 
    
    return df

def conditional_stop_signal_post(df):
    df.insert(0,'stopped',df['key_press'] == -1)
    df.loc[:,'correct'] = (df['key_press'] == df['correct_response']).astype(float)
    return df

def DPX_post(df):
    df.loc[:,'correct'] = df['correct'].astype(float)
    index = df[(df['trial_id'] == 'fixation') & (df['possible_responses'] != 'none')].index
    if len(index) > 0:
        df.loc[index,'fixation'] = 'none'
    return df

def stop_signal_post(df):
    df.insert(0,'stopped',df['key_press'] == -1)
    df.loc[:,'correct'] = (df['key_press'] == df['correct_response']).astype(float)
    return df  

def stroop_post(df):
    df.loc[:,'correct'] = df['correct'].astype(float)
    return df

def twobytwo_post(df):
    df.insert(0, 'CTI', pandas.Series(data = df[df['trial_id'] == "cue"].block_duration.tolist(), \
                                        index = df[df['trial_id'] == "stim"].index))
    switch_i = df[(df['task_switch'] != 'stay')].index
    df.loc[switch_i, 'cue_switch'] = numpy.nan
    df.loc[:, 'switch_type'] = df.task_switch.apply(lambda x: 'task_' + str(x))
    # task stay trials
    stay_i = df[(df['task_switch'] == 'stay')].index
    df.loc[stay_i, 'switch_type'] = df.loc[stay_i].cue_switch \
                                    .apply(lambda x: 'cue_' + str(x))
    return df

def WATT_post(df):
    # correct bug where exp stage is incorrectly labeled practice in some trials
    test_index = df.loc[:,'condition'].apply(lambda x: x in  ['PA_with_intermediate', 'PA_without_intermediate'])
    df.loc[test_index,'exp_stage']='test'
    # add problem id to feedback rows
    index = df.query('trial_id == "feedback"').index
    i_index = [df.index.get_loc(i)-1 for i in index]
    df.loc[index,'problem_id'] = df.iloc[i_index]['problem_id'].tolist()
    df.loc[index,'condition'] = df.iloc[i_index]['condition'].tolist()
    return df

#***********************************
# CREATE CLEANED DATAFRAMES
#***********************************

def clean_data(df, exp_id = None, apply_post = True, drop_columns = None):
    '''clean_df returns a pandas dataset after removing a set of default generic 
    columns. Optional variable drop_cols allows a different set of columns to be dropped
    :df: a pandas dataframe
    :param experiment: a string identifying the experiment used to automatically drop unnecessary columns. df should not have multiple experiments if this flag is set!
    :param apply_post: bool, if True apply post-processig function retrieved using apply_post
    :param drop_columns: a list of columns to drop. If not specified, a default list will be used from utils.get_dropped_columns()
    :param lookup: bool, default true. If True replaces all values in dataframe using the lookup_val function
    :param return_reject: bool, default false. If true returns a dataframe with rejected experiments
    '''
    if apply_post:
        # apply post processing 
        df = post_process_exp(df, exp_id)
            
    # Drop unnecessary columns
    if drop_columns == None:
        drop_columns = get_drop_columns()   
    df.drop(drop_columns, axis=1, inplace=True, errors='ignore')
    if exp_id != None:
        drop_rows = get_drop_rows(exp_id)
        # Drop unnecessary rows, all null rows
        for key in drop_rows.keys():
            df = df.query('%s not in  %s' % (key, drop_rows[key]))
    df = df.dropna(how = 'all')
    #drop columns with only null values
    drop_null_cols(df)
    
    return df

def get_drop_columns():
    return ['view_history', 'trial_index', 'internal_node_id', 
        'stim_duration', 'block_duration', 'feedback_duration','timing_post_trial', 
        'test_start_block','exp_id']
        
def get_drop_rows(exp_id):
    '''Function used by clean_df to drop rows from dataframes with one experiment
    :experiment: experiment key used to look up which rows to drop from a dataframe
    '''
    gen_cols = ['welcome', 'text','instruction', 'attention_check','end', 'post task questions', 'fixation', \
                'practice_intro', 'rest', 'rest_block', 'test_intro', 'task_setup', 'test_start_block'] #generic_columns to drop
    no_test_start_block = ['welcome', 'text','instruction', 'attention_check','end', 'post task questions', 'fixation', \
                'practice_intro', 'rest', 'rest_block', 'test_intro', 'task_setup']
    lookup = {'adaptive_n_back': {'trial_id': gen_cols + ['update_target', 'update_delay', 'delay_text']},
            'attention_network_task': {'trial_id': gen_cols + ['spatialcue', 'centercue', 'doublecue', 'nocue', 'rest block', 'intro']},
            'columbia_card_task_cold': {'trial_id': gen_cols + ['calculate reward','reward','end_instructions']}, 
            'columbia_card_task_hot': {'trial_id': gen_cols + ['calculate reward', 'reward', 'test_intro']}, 
            'columbia_card_task_fmri': {'trial_id': gen_cols + ['calculate reward', 'reward']}, 
            'directed_forgetting': {'trial_id': gen_cols + ['ITI_fixation', 'intro_test', 'stim', 'cue', 'instruction_images']},
            'discount_fixed': {'trial_id': gen_cols},
            'dot_pattern_expectancy': {'trial_id': gen_cols + ['instruction_images', 'feedback']},
            'go_nogo': {'trial_id': gen_cols + ['reset_trial']},
            'motor_selective_stop_signal': {'trial_id': gen_cols + ['prompt_fixation', 'feedback']},
            'stop_signal': {'trial_id': gen_cols + ['reset', 'feedback']},
            'stroop': {'trial_id': gen_cols + []}, 
            'survey_medley': {'trial_id': gen_cols},
            'twobytwo': {'trial_id': no_test_start_block + ['cue', 'gap', 'set_stims']},
            'tower_of_london': {'trial_id': gen_cols + ['advance', 'practice']},
            'ward_and_allport': {'trial_id': gen_cols + ['practice_start_block', 'reminder', 'test_start_block']}
    } 
    to_drop = lookup.get(exp_id, {})
    return to_drop

def post_process_exp(df, exp_id):
    '''Function used to post-process a dataframe extracted via extract_row or extract_experiment
    :exp_id: experiment key used to look up appropriate grouping variables
    '''
    lookup = {'attention_network_task': ANT_post,
            'columbia_card_task_fmri': CCT_fmri_post,
            'dot_pattern_expectancy': DPX_post,
            'motor_selective_stop_signal': conditional_stop_signal_post,
            'stop_signal': stop_signal_post,
            'stroop': stroop_post,
            'twobytwo': twobytwo_post,
            'ward_and_allport': WATT_post}     
                
    fun = lookup.get(exp_id, lambda df: df)
    return fun(df).sort_index(axis = 1)
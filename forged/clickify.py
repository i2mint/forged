import numpy as np
from librosa.util.utils import pad_center
from librosa.util.exceptions import ParameterError

def pad_to_length(wf_chk, length):
    wf_chk = np.array(wf_chk)
    try:
        return pad_center(wf_chk, length, mode='constant')
    except ParameterError as err:
        print(err)

def shift_to_ts(wf_click, ts, wf_marker):
    if ts not in range(len(wf_click)):
        raise ParameterError('timestamp outside of range of wf')
    delta = ts-wf_marker
    return np.roll(wf_click, delta)

def check_equal_length(wf1, wf2):
    return len(wf1)==len(wf2)

def clickify_one(wf, timestamp, wf_click, click_marker = None):
    if click_marker is None:
        click_marker = np.argmax(np.abs(wf_click))
    
    shifted_click = shift_to_ts(wf_click, timestamp, click_marker)
    return wf + shifted_click

def add_signals():
    pass # careful to wraparounds
    
def clickify(wf, timestamps, wf_click, click_marker = None):
    if click_marker is None:
        click_marker = np.argmax(np.abs(wf_click))
    shifted_clicks = [shift_to_ts(wf_click, timestamp, click_marker) for timestamp in timestamps]    
    return wf + sum(shifted_clicks)
    
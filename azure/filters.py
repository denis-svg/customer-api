from scipy.signal import find_peaks
import numpy as np
from statistics import median
# from pykalman import KalmanFilter


def filter1(array, flag=False):
    if not flag:
        peaks, _ = find_peaks(np.array(array), width=1, height=1, threshold=1)
    else:
        peaks, _ = find_peaks(np.array(array))
    if peaks.size > 0:
        array = [array[i] for i in range(len(array)) if i not in peaks]
    return array

def filter2(array, flag=False):
    if not flag:
        peaks, _ = find_peaks(np.array(array), width=1, height=1, threshold=1)
    else:
        peaks, _ = find_peaks(np.array(array))
    if peaks.size > 0:
        threshold = median(np.array(array)[peaks])
        array = [top for top in array if top <= threshold]
    return array

def filter3(array, flag=False):
    if not flag:
        peaks, _ = find_peaks(np.array(array), width=1, height=1, threshold=1)
    else:
        peaks, _ = find_peaks(np.array(array))
    if peaks.size > 0:
        threshold = median(np.array(array)[peaks])
        array = [int(top) if top <= threshold else int(threshold) for top in array]
    return array
"""
Utils to create synthetic streams with drifts
"""


import numpy as np


random_state = np.random.RandomState(seed=42)
dist_a = random_state.normal(0.8, 0.05, 1000)
dist_b = random_state.normal(0.4, 0.02, 1000)
dist_c = random_state.normal(0.6, 0.1, 1000)

# Concatenate data to simulate a data stream with 2 drifts
dflt_stream = np.concatenate((dist_a, dist_b, dist_c))


def simple_shift_drift():
    """
    distribution with 2 shifts
    """
    return dflt_stream

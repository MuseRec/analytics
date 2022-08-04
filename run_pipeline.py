"""
    Analytics Pipeline

    Derives features from the interaction data, e.g., the number of artworks
    visited per part, the dwell time on artworks, and the length of time
    spent in each part.
"""
import numpy as np 
import pandas as pd
from dateutil.parser import parse

from pipeline.pipeline import Pipeline

np.random.seed(42)


def _get_data():
    # read in the interaction data
    interactions = pd.read_csv(
        'data/interaction_data.csv'
    ).sort_values(['user_id', 'timestamp']).to_dict(orient = 'records') 

    # read in the user data
    users = pd.read_csv(
        'data/users.csv', 
        usecols = ['user_id', 'distraction_task_timestamp']
    ).to_dict(orient = 'records')

    # convert into a dictionary for ease look-up
    user_distraction_times = {
        u['user_id']: parse(u['distraction_task_timestamp'])
        for u in users
    }

    data = {
        user: {'part_one': [], 'part_two': []} 
        for user in user_distraction_times.keys()
    }
    for event in interactions:
        dt_time = user_distraction_times[event['user_id']]
        timestamp = parse(event['timestamp'])

        if dt_time > timestamp:
            data[event['user_id']]['part_one'].append({
                'content_id': event['content_id'],
                'event': event['event'],
                'page': event['page'],
                'timestamp': timestamp,
                'part': 'part_one'
            })
        else:
            data[event['user_id']]['part_two'].append({
                'content_id': event['content_id'],
                'event': event['event'],
                'page': event['page'],
                'timestamp': timestamp,
                'part': 'part_two'
            })

    return data


def main():
    data = _get_data()
    pipe = Pipeline(data)

    statistics = pipe.pipe()
    statistics = pipe.to_dataframe()

    statistics.to_csv('data/statistics.csv', index = False)


if __name__ == '__main__':
    main()



import random as rand
import numpy as np 

from pandas import DataFrame
from typing import Dict
from collections import Counter

rand.seed(42)
np.random.seed(42)

TIME_OF_DAY_MAPPING = {
    1: 'Late Night', 2: 'Early Morning', 3: 'Morning', 
    4: 'Noon', 5: 'Evening', 6: 'Night'
}

class Pipeline:

    def __init__(self, data) -> None:
        self.data = data 
        self.statistics = {user: {} for user in self.data.keys()}

    
    def _temporal_features(self):
        """
        """
        def _time_spend_on_artworks(user_events):
            """
            """
            # get the click onto an artwork event and 'go-back' pairs
            pairs = []
            for indx, event in enumerate(user_events):
                content_id = event['content_id']
                if event['event'] == 'click':
                    # search forward for the go-back event for same content
                    for fwd_event in user_events[indx:]:
                        if (content_id == fwd_event['content_id'] and 
                            fwd_event['event'] == 'go-back'):
                            pairs.append((event, fwd_event))

            differences = [
                (back['timestamp'] - click['timestamp']).seconds
                for click, back in pairs 
            ]

            return round(np.mean(differences), 3), round(np.std(differences), 3)


        for user, events in self.data.items():
            part_one = events['part_one']
            part_two = events['part_two']

            part_one_time = (
                part_one[-1]['timestamp'] - part_one[0]['timestamp']).seconds
            part_two_time = (
                part_two[-1]['timestamp'] - part_two[0]['timestamp']).seconds

            part_one_artwork_time_m, part_one_artwork_time_std = _time_spend_on_artworks(part_one)
            part_two_artwork_time_m, part_two_artwork_time_std = _time_spend_on_artworks(part_two)

            self.statistics[user].update({
                'part_one_time': round(part_one_time / 60.0, 3),
                'part_two_time': round(part_two_time / 60.0, 3),
                'overall_time': round((part_one_time + part_two_time) / 60.0, 3),
                'part_one_artwork_time_m': part_one_artwork_time_m,
                'part_one_artwork_time_std': part_one_artwork_time_std,
                'part_two_artwork_time_m': part_two_artwork_time_m,
                'part_two_artwork_time_std': part_two_artwork_time_std
            })

        return self.statistics


    def _descriptive_statistics(self):
        """
        """
        def _event_count(user_events, event):
            return len([
                ev['event'] for ev in user_events
                if ev['event'] == event
            ])

        def _artwork_revisits(user_events):
            # count all of the artwork click events
            click_events_counter = Counter([
                event['content_id'] for event in user_events 
                if (event['event'] == 'click') and 
                    (event['content_id'] != 'home-button')
            ])

            return len([
                event for event, c in click_events_counter.items() if c > 1
            ])

        def _artworks_visited_before_first_choice(user_events):
            """
                Get the number of artworks visited and the time
            """
            # get the first artwork selected event
            first_selection_event = None
            first_selection_event_index = 0
            for indx, event in enumerate(user_events):
                if event['event'] == 'artwork-selected':
                    first_selection_event = event 
                    first_selection_event_index = indx
                    break 
            
            # get the number of artworks visited before the first selection
            count = 0
            if first_selection_event:
                for event in user_events[:first_selection_event_index]:
                    if (event['event'] == 'click' and 
                        event['content_id'] != 'home-button'):
                        count += 1

            # get the time difference between the first event and the first 
            # selection event
            time_diff = (
                first_selection_event['timestamp'] - user_events[0]['timestamp']
            ).seconds / 60.0

            return count, round(time_diff, 3)


        for user, events in self.data.items(): 
            # get the number of artworks visited for each part               
            num_artworks_part_one = len({
                event['content_id']
                for event in events['part_one'] 
                if event['content_id'] != 'home-button'
            })
            num_artworks_part_two = len({
                event['content_id']
                for event in events['part_two']
                if event['content_id'] != 'home-button'
            })

            # get the individual event counts for each part
            part_one_event_counts = {
                event_name.replace('-', '_') + '_part_one': _event_count(
                    events['part_one'], event_name
                )
                for event_name in [
                    'show-more', 'artwork-selected', 'artwork-deselected'
                ]
            }
            part_two_event_counts = {
                event_name.replace('-', '_') + '_part_two': _event_count(
                    events['part_two'], event_name
                )
                for event_name in [
                    'show-more', 'artwork-selected', 'artwork-deselected'
                ]
            }
            event_counts = {**part_one_event_counts, **part_two_event_counts}

            pone_num_visited_before, pone_time_before = _artworks_visited_before_first_choice(events['part_one'])
            ptwo_num_visited_before, ptwo_time_before = _artworks_visited_before_first_choice(events['part_two'])

            self.statistics[user].update({
                'num_artworks_part_one': num_artworks_part_one,
                'num_artworks_part_two': num_artworks_part_two,
                'total_events': sum(event_counts.values()),
                'num_revisits_part_one': _artwork_revisits(events['part_one']),
                'num_revisits_part_two': _artwork_revisits(events['part_two']),
                'num_visited_before_first_choice_part_one': pone_num_visited_before,
                'time_before_first_choice_part_one': pone_time_before,
                'num_visited_before_first_choice_part_two': ptwo_num_visited_before,
                'time_before_first_selection_part_two': ptwo_time_before,
                **event_counts
            })

        return self.statistics
            
    
    def pipe(self) -> Dict:
        self.statistics = self._temporal_features()
        self.statistics = self._descriptive_statistics()

        return self.statistics

    
    def to_dataframe(self) -> DataFrame:
        return DataFrame.from_dict(
            self.statistics, orient = 'index'
        ).reset_index().rename(columns = {'index': 'user_id'})
from collections import defaultdict
from datetime import datetime, timedelta
import pickle
import time

def working_hours_inconsistency(commits):
    suspicious_patterns = []
    working_hours = range(9, 18)  # 9 AM to 6 PM
    
    # Group commits by timezone
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit[2]
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():
        tz_offset_hour = commit[2]//60# Extract hour offset
        
        # Count commits by hour
        hour_distribution = defaultdict(int)
        for commit in tz_commits:
            local_hour = (commit[1].hour + tz_offset_hour) % 24
            hour_distribution[local_hour] += 1
        
        total_commits = len(tz_commits)
        if total_commits < 5:  # Skip if too few commits ARBITRARY
            # TODO Count
            continue
            
        # Calculate percentage of commits outside working hours
        outside_hours_commits = sum(
            count for hour, count in hour_distribution.items()
            if hour not in working_hours
        )
        outside_ratio = outside_hours_commits / total_commits
        
        # Flag if more than 40% of commits are outside working hours ARBITRARY
        if outside_ratio > 0.4:
            suspicious_patterns.append({
                'timezone': tz,
                'outside_ratio': round(outside_ratio, 2),
                'total_commits': total_commits
            })
        # TODO Count
    return suspicious_patterns
            
def weekend_hours_inconsistency(commits):
    suspicious_patterns = []
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit[2]
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():
        total_commits = len(tz_commits)

        weekend_commits = sum(
            1 for commit in tz_commits
            if commit[1].weekday() >= 5  # 5=Saturday, 6=Sunday
        )
        weekend_ratio = weekend_commits / total_commits
        
        # Flag if more than 30% of commits are on weekends ARBITRARY
        if weekend_ratio > 0.3:
            suspicious_patterns.append({
                'timezone': tz,
                'weekend_ratio': round(weekend_ratio, 2),
                'total_commits': total_commits
            })
    return suspicious_patterns
    
def get_commit_session(commit_time, tz_offset):
    """Categorize time into morning/afternoon/evening/night"""
    local_hour = (commit_time.hour + tz_offset) % 24
    if 5 <= local_hour < 12:
        return 'morning'
    elif 12 <= local_hour < 18:
        return 'afternoon'
    elif 18 <= local_hour < 22:
        return 'evening'
    else:
        return 'night'
    
def schedule_insconsistency(commits):
    suspicious_patterns = []
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit[2]
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():  
        tz_offset_hour = commit[2]//60 
        # Track consecutive sessions
        commits_by_date = defaultdict(list)
        for commit in sorted(tz_commits, key=lambda x: x[1]):
            date = commit[1].date()
            session = get_commit_session(commit[1], tz_offset_hour)
            commits_by_date[date].append(session)
        
        # Look for inconsistent patterns
        for date, sessions in commits_by_date.items():
            # Flag if commits span across too many different sessions ARBITRARY
            if len(set(sessions)) >= 3:
                suspicious_patterns.append({
                    'timezone': tz,
                    'date': date,
                    'sessions': set(sessions),
                    'commit_count': len(sessions)
                })
    return suspicious_patterns

def activites_inconsistency(commits):
    suspicious_patterns = []
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit[2]
        timezone_commits[tz].append(commit)
    if len(timezone_commits) >= 2:
        commits_per_tz = {
            tz: len(commits) 
            for tz, commits in timezone_commits.items()
        }
        max_commits_tz = max(commits_per_tz.items(), key=lambda x: x[1])
        
        # For each timezone, compare against the dominant timezone
        for tz, count in commits_per_tz.items():
            if tz == max_commits_tz[0]:
                continue
                
            # Flag if a non-dominant timezone has >25% of commits ARBITRARY
            ratio_to_dominant = count / max_commits_tz[1]
            if ratio_to_dominant > 0.25:
                suspicious_patterns.append({
                    'timezone': tz,
                    'dominant_timezone': max_commits_tz[0],
                    'commit_ratio': round(ratio_to_dominant, 2),
                    'commit_count': count
                })
    return suspicious_patterns

def aba_pattern(commits):
    suspicious_patterns = []
    commits = sorted(commits, key=lambda commit: commit[1])
    commitA = None
    commitB = None
    
    for commit in commits:
        current_tz = commit[2]
        if commitA == None:
            commitA = commit
            continue
        
        if commitB == None and current_tz != commitA[2]:
            commitB = commit
            continue
        
        if commitB != None and current_tz == commitA[2]:
            suspicious_patterns.append({
                'dates': (commitA[1], commitB[1], commit[1]),
                'timezones': (commitA[2], commitB[2], current_tz),
                'commit hash': (commitA[0], commitB[0], commit[0])
            })
            commitA = commitB
            commitB = commit
            continue
        commitA = commit
        commitB = None

    return suspicious_patterns

def travel_time(commits):
    pass

def dbscan_res(commits):
    pass

def remove_consecutive_duplicates(sequence):
    if not sequence:
        return []
    result = [sequence[0]]
    for item in sequence[1:]:
        if item != result[-1]:
            result.append(item)
    return result

def window_tz_inconsistency(commits, window_range=24 * 60 * 60):
    """
    commits are already sorted
    """
    suspicious_pattern = []
    last_commit = None
    for i in range(len(commits)):
        current_window = [commits[i]]
        for j in range(i+1, len(commits)):
            time_diff = commits[j][1] - current_window[0][1]
            if time_diff.total_seconds() > window_range:
                break
            current_window.append(commits[j])
        if len(current_window) > 2 and last_commit != current_window[-1]: # at least 3 commits in the window and a bigger window
            tzs = [commit[2] for commit in current_window]
            if len(remove_consecutive_duplicates(tzs)) > 2:
                suspicious_pattern.append(current_window)
        last_commit = current_window[-1]
    return suspicious_pattern

if __name__ == "__main__":
    db = {}
    with open('db.pkl', 'rb') as f:
        db = pickle.load(f)
    for aut in db:
        db[aut] = sorted(db[aut], key=lambda val: val[1])
        
    amount_author = len(db)
    
    amount_window_tz_inconsistency = []
    amount_of_skip = 0
    for author, commits in db.items():
        if len(commits)>10000:
            amount_of_skip += 1
            continue
        sp = window_tz_inconsistency(commits)
        if len(sp)>0:
            amount_window_tz_inconsistency.append(author)
    print("Amount of author flagged: ", len(amount_window_tz_inconsistency))
    print("Ration of author changing timezones at least 2 times in a given window: ", round(len(amount_window_tz_inconsistency)/amount_author*100, 2), "%")
    print("Amount of author having more than 10,000 commits than I skipped: ", amount_of_skip)
    for res in amount_window_tz_inconsistency:
        print(res)

    """ start_time = time.time()
    amount_window_tz_inconsistency = 0
    for author, commits in db.items():
        sp = window_tz_inconsistency(commits)
        if len(sp)>0:
            amount_window_tz_inconsistency += 1
    print("Ration of author changing timezones at least 2 times in a given window: ", round(amount_window_tz_inconsistency/amount_author*100, 2), "%")
    print("--- %s seconds ---" % (time.time() - start_time)) """
    
    """ start_time = time.time()
    amount_window_tz_inconsistency = 0
    for author, commits in db.items():
        sp = window_tz_inconsistency(commits, 7*24*60*60)
        if len(sp)>0:
            amount_window_tz_inconsistency += 1
    print("Ration of author changing timezones at least 2 times in a given window: ", round(amount_window_tz_inconsistency/amount_author*100, 2), "%")
    print("--- %s seconds ---" % (time.time() - start_time)) """
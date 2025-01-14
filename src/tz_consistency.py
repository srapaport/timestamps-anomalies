from collections import defaultdict
from datetime import datetime, timedelta
import pickle

def working_hours_inconsistency(commits):
    suspicious_patterns = defaultdict(list)
    working_hours = range(9, 18)  # 9 AM to 6 PM
    
    # Group commits by timezone
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit['timezone']
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():
        tz_offset = int(tz[:3])  # Extract hour offset
        
        # Count commits by hour
        hour_distribution = defaultdict(int)
        for commit in tz_commits:
            local_hour = (commit['timestamp'].hour + tz_offset) % 24
            hour_distribution[local_hour] += 1
        
        total_commits = len(tz_commits)
        # if total_commits < 5:  # Skip if too few commits
        #     continue
            
        # Calculate percentage of commits outside working hours
        outside_hours_commits = sum(
            count for hour, count in hour_distribution.items()
            if hour not in working_hours
        )
        outside_ratio = outside_hours_commits / total_commits
        
        # Flag if more than 40% of commits are outside working hours
        if outside_ratio > 0.4:
            suspicious_patterns['high_outside_hours'].append({
                'timezone': tz,
                'outside_ratio': round(outside_ratio, 2),
                'total_commits': total_commits
            })
    return suspicious_patterns
            
def weekend_hours_inconsistency(commits):
    suspicious_patterns = defaultdict(list)
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit['timezone']
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():
        total_commits = len(tz_commits)

        weekend_commits = sum(
            1 for commit in tz_commits
            if commit['timestamp'].weekday() >= 5  # 5=Saturday, 6=Sunday
        )
        weekend_ratio = weekend_commits / total_commits
        
        # Flag if more than 30% of commits are on weekends
        if weekend_ratio > 0.3:
            suspicious_patterns['high_weekend_activity'].append({
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
    elif 12 <= local_hour < 17:
        return 'afternoon'
    elif 17 <= local_hour < 22:
        return 'evening'
    else:
        return 'night'
    
def schedule_insconsistency(commits):
    suspicious_patterns = defaultdict(list)
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit['timezone']
        timezone_commits[tz].append(commit)
    
    for tz, tz_commits in timezone_commits.items():  
        tz_offset = int(tz[:3])      
        # Track consecutive sessions
        commits_by_date = defaultdict(list)
        for commit in sorted(tz_commits, key=lambda x: x['timestamp']):
            date = commit['timestamp'].date()
            session = get_commit_session(commit['timestamp'], tz_offset)
            commits_by_date[date].append(session)
        
        # Look for inconsistent patterns
        for date, sessions in commits_by_date.items():
            if len(sessions) < 3:
                continue
                
            # Flag if commits span across too many different sessions
            unique_sessions = len(set(sessions))
            if unique_sessions >= 3:
                suspicious_patterns['inconsistent_schedule'].append({
                    'timezone': tz,
                    'date': date,
                    'sessions': unique_sessions,
                    'commit_count': len(sessions)
                })
    return suspicious_patterns

def activites_inconsistency(commits):
    suspicious_patterns = defaultdict(list)
    timezone_commits = defaultdict(list)
    for commit in commits:
        tz = commit['timezone']
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
                
            # Flag if a non-dominant timezone has >25% of commits
            ratio_to_dominant = count / max_commits_tz[1]
            if ratio_to_dominant > 0.25:
                suspicious_patterns['multiple_active_timezones'].append({
                    'timezone': tz,
                    'dominant_timezone': max_commits_tz[0],
                    'commit_ratio': round(ratio_to_dominant, 2),
                    'commit_count': count
                })
    return suspicious_patterns

def aba_pattern(commits):
    pass

def travel_time(commits):
    pass

def dbscan_res(commits):
    pass

if __name__ == "__main__":
    db = {}
    with open('notebook/db.pkl', 'rb') as f:
        db = pickle.load(f)
use std::collections::{HashMap, HashSet};

pub fn two_by_two_hour_diff_old(author:String, dates: Vec<(String, i64, i16)>) -> HashMap<i64, HashMap<String, HashSet<(String, String)>>> {
    let mut res: HashMap<i64, HashMap<String, HashSet<(String, String)>>> = HashMap::new();
    let mut prev_id = None;
    let mut prev_ts = None;
    let mut prev_tz = None;
    for (curr_id, curr_ts, curr_tz) in dates{
        if prev_id == None{
            prev_id = Some(curr_id);
            prev_ts = Some(curr_ts);
            prev_tz = Some(curr_tz);
            continue;
        }
        if curr_tz != prev_tz.unwrap(){
            for hour in (1..25).rev(){
                if (curr_ts - prev_ts.unwrap()) < (hour * 60 * 60){
                    res.entry(hour)
                        .or_insert_with(HashMap::new)
                        .entry(author.clone())
                        .or_insert_with(HashSet::new)
                        .insert((format!("swh:1:rev:{}", prev_id.clone().unwrap()), format!("swh1:rev:{}", curr_id)));
                }
            }
        }
        prev_id = Some(curr_id);
        prev_ts = Some(curr_ts);
        prev_tz = Some(curr_tz);
    }
    res
}

pub fn two_by_two(author:String, dates: Vec<(String, i64, i16)>) -> HashMap<String, HashSet<(String, i64, i16, String, i64, i16)>> {
    let mut res: HashMap<String, HashSet<(String, i64, i16, String, i64, i16)>> = HashMap::new();
    let mut prev_id = None;
    let mut prev_ts = None;
    let mut prev_tz = None;
    for (curr_id, curr_ts, curr_tz) in dates{
        if prev_id == None{
            prev_id = Some(curr_id);
            prev_ts = Some(curr_ts);
            prev_tz = Some(curr_tz);
            continue;
        }
        if curr_tz != prev_tz.unwrap(){
            res.entry(author.clone())
                .or_insert_with(HashSet::new)
                .insert((format!("swh:1:rev:{}", prev_id.clone().unwrap()), prev_ts.unwrap(), prev_tz.unwrap(), format!("swh1:rev:{}", curr_id), curr_ts, curr_tz));
        }
        prev_id = Some(curr_id);
        prev_ts = Some(curr_ts);
        prev_tz = Some(curr_tz);
    }
    res
}

pub fn more_than_one_timezone(dates: &HashSet<(String, i64, i16)>)->bool{
    let mut prev_tz = dates.iter().next().unwrap().1;
    for date in dates{
        if date.1 != prev_tz{
            return true;
        }
    }
    false
}

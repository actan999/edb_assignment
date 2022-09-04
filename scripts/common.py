from datetime import datetime, timezone, timedelta, date
import time
import requests
import json
import pandas as pd
import datetime as dt
import sqlite3
from pandas.api.types import CategoricalDtype



six_mth_period = [('2022-03-01', '2022-03-14'),
                ('2022-03-15', '2022-03-27'),
                ('2022-03-28', '2022-04-09'),
                ('2022-04-10', '2022-04-22'),
                ('2022-04-23', '2022-05-05'),
                ('2022-05-06', '2022-05-18'),
                ('2022-05-19', '2022-05-31'),
                ('2022-06-01', '2022-06-13'),
                ('2022-06-14', '2022-06-26'),
                ('2022-06-27', '2022-07-09'),
                ('2022-07-10', '2022-07-22'),
                ('2022-07-23', '2022-08-05'),
                ('2022-08-06', '2022-08-18'),
                ('2022-08-19', '2022-08-31'),]


three_hourly_blocks = [
    ('00-03', '00:00:00' , '02:59:00'),
    ('03-06', '03:00:00' , '05:59:00'),
    ('06-09', '06:00:00' , '08:59:00'),
    ('09-12', '09:00:00' , '11:59:00'),
    ('12-15', '12:00:00' , '14:59:00'),
    ('15-18', '15:00:00' , '17:59:00'),
    ('18-21', '18:00:00' , '20:59:00'),
    ('21-00', '21:00:00' , '23:59:00'),
]


ordered_weekday = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']



def pause_until(time):
    """ 
    Pause your program until a specific end time. 'time' is either
    a valid datetime object or Unix timestamp in seconds (i.e. seconds
    since Unix epoch) 
    """
    end = time

    # Convert datetime to Unix timestamp and adjust for locality
    if isinstance(time, datetime):
        # If we're on Python 3 and the user specified a timezone, convert to UTC and get timestamp
        if time.tzinfo:
            end = time.astimezone(timezone.utc).timestamp()
        else:
            zone_diff = datetime.now() - ((datetime.now() - datetime(1970, 1, 1)).total_seconds())
            end = (time - datetime(1970, 1, 1)).total_seconds() + zone_diff

    if not isinstance(end, (int, float, datetime)):
        raise Exception('The time parameter is not a number or datetime object')

    while True: # Now we wait
        now = datetime.now()
        diff = end - now
        if diff <= 0: # Time's up!
            break
        else:          
            time.sleep(diff / 2)   # 'logarithmic' sleeping to minimize loop iterations


def manage_rate_limit(response):
    """    
    Takes in a `requests` response object after querying
    API and uses the headers["X-RateLimit-Remaining"] and
    headers["X-RateLimit-Reset"] headers objects to manage the API
    most common, time-dependent HTTP errors.
    """
    
    while True:
        remaining_requests = int(response.headers["X-RateLimit-Remaining"]) # Get number of requests left with our tokens

        # If that number is one, we get the reset-time and wait until then, add 15 seconds
        if remaining_requests == 1:
            buffer_wait_time = 15
            resume_time = datetime.fromtimestamp( int(response.headers["X-RateLimit-Reset"]) + buffer_wait_time)
            print(f"One request from being rate limited. Waiting on endpoint.\n\tResume Time: {resume_time}")
            pause_until(resume_time)

        # Explicitly checking for time dependent errors.
        # Time errors can be solved by waiting a little and pinging again
        if response.status_code != 200:
            if response.status_code == 429:
                buffer_wait_time = 15
                resume_time = datetime.fromtimestamp( int(response.headers["x-rate-limit-reset"]) + buffer_wait_time )
                print(f"Too many requests. Waiting on endpoint.\n\tResume Time: {resume_time}")
                pause_until(resume_time)

            # internal server error
            elif response.status_code == 500:
                # Twitter needs a break, so we wait 30 seconds
                resume_time = datetime.now().timestamp() + 30
                print(f"Internal server error @ endpoint. Giving endpoint a break...\n\tResume Time: {resume_time}")
                pause_until(resume_time)

            # service unavailable error
            elif response.status_code == 503:
                # Endpoint needs a break, so we wait 30 seconds
                resume_time = datetime.now().timestamp() + 30
                print(f"Service unavailable. Giving endpoint a break...\n\tResume Time: {resume_time}")
                pause_until(resume_time)

            # If we get this far, we should exit
            raise Exception(
                f"Request returned an error: {response.status_code} {response.text}")

        # Each time we get a 200 response, exit the function and return the response object
        if response.ok:
            return response


def call_api(url, headers):
    response = manage_rate_limit(requests.get(url, headers))
    return json.loads(response.content)


def collect_data(base_query_url, headers, date_coverage):
    response_container = {}
    for start_date, end_date in date_coverage:
        time.sleep(60)
        url = f"{base_query_url} committer-date:{start_date}..{end_date}&per_page=100"
        response_container[f"{start_date} to {end_date}"] = call_api(url, headers)
    return response_container
        

def query_sqlite3_db(sql_db, sql_string):
    with sqlite3.connect(sql_db) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string)
        conn.commit()


def seek_consecutive_dates(sorted_date_sequence_in_yyyy_mm_dd_format):
    dates = [datetime.strptime(d, "%Y-%m-%d") for d in sorted_date_sequence_in_yyyy_mm_dd_format]
    dates_integer = [date.toordinal() for date in dates]  # day count from the date 01/01/01
    ranges = {}
    a_range = []
    prev_integer = 0
    index = 0
    j = 1
    for integer in dates_integer:
        if (integer + 1 == dates_integer[index] + 1) and (integer - 1 == prev_integer): # if integers are consecutive, forward & backward
            a_range.append(dates[index].strftime("%Y-%m-%d"))
        elif prev_integer == 0:  # apend 1st date to a_range list since 'prev_integer' has not been updated
            a_range.append(dates[index].strftime("%Y-%m-%d"))
        else:
            ranges.update({f'Range{j}': tuple(a_range)}) # integer no longer consecutive, update dict with new range
            a_range = []                                    # clear and start appending to next range
            j += 1 
            a_range.append(dates[index].strftime("%Y-%m-%d"))
        index += 1
        prev_integer = integer
    ranges.update({f'Range{j}': tuple(a_range)})
    return ranges


def localize_timestamp_to_local_timezone(df, utc_time_col, local_timezone):
    df['utc_dt_local_tz'] = pd.to_datetime(df[utc_time_col], utc=True, errors='coerce').dt.tz_convert(local_timezone)
    df['utc_dt_isoformat'] = df['utc_dt_local_tz'].dt.strftime('%Y-%m-%d')
    df['locale_date'] = df['utc_dt_local_tz'].dt.strftime('%x')  # mm/dd/yy as SG convention
    df.sort_values(by='utc_dt_isoformat', inplace=True) # to ease looking for consecutive dates
    return df


def day_of_week(df, datetime64_ns_col):
    df['day_of_week'] = df[datetime64_ns_col].dt.strftime('%a')
    return df


def clock_time(df, datetime64_ns_col):
    df['clock_time'] = df[datetime64_ns_col].dt.strftime('%X')
    return df


def time_partitioning(df, time_blocks):
    df['time_block'] = ""
    for time_block, start, end in time_blocks:
        mask = (df['clock_time'] > start) & (df['clock_time'] < end)
        idx = df[mask].index
        for i in idx:
            df.loc[i, ['time_block']] = time_block
    return df


def ordering_weekdays(df, day_of_week_col, ordered_weekday):
    cat_type = CategoricalDtype(categories=ordered_weekday, ordered=True)
    df[day_of_week_col] = df[day_of_week_col].astype(cat_type)
    return df



def df_for_heatmap(df, local_timezone):
    issue_3_df = localize_timestamp_to_local_timezone(df.copy(), 'commit_datetime', local_timezone)
    issue_3_df = day_of_week(issue_3_df, 'utc_dt_local_tz')
    issue_3_df = clock_time(issue_3_df, 'utc_dt_local_tz')
    issue_3_df['datetime_index'] = pd.to_datetime(issue_3_df['utc_dt_local_tz'])
    issue_3_df_time_indexed = issue_3_df.set_index('datetime_index')
    issue_3_df_time_indexed = time_partitioning(issue_3_df_time_indexed, three_hourly_blocks)
    issue_3_df_time_indexed = ordering_weekdays(issue_3_df_time_indexed, 'day_of_week', ordered_weekday)
    issue_3_df_time_indexed.asfreq('Min')
    return issue_3_df_time_indexed


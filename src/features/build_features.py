import pandas as pd
import numpy as np
import timeit

rolling_window_size = 5
rolling_window_time = '10s' # in seconds

def calculate_features(df):
    # connection based features
    # 2. Number of times a specific SRCADDRESS has appeared in the last X flow records
    df['conn_src_adr_count'] = df['Src IP'].rolling(rolling_window_size).apply(
        lambda x: np.count_nonzero(x == x[rolling_window_size - 1]))

    # 3. Number of distinct destination ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
    df['conn_dst_port_for_src_adr_count'] = df.groupby('Src IP')['Dst Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).apply(lambda y: len(set(y))))

    # 4. Number of distinct destination IPs for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
    df['conn_dst_adr_for_src_adr_count'] = df.groupby('Src IP')['Dst IP'].apply(lambda x: x.replace('.', '')).rolling(
        rolling_window_size).apply(lambda x: len(set(x)))

    # 5. Number of distinct source ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
    df['conn_src_port_for_src_adr_count'] = df.groupby('Src IP')['Src Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).apply(lambda y: len(set(y))))

    # 6. Average number of bytes for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
    # TODO: format csv to have bytes

    # 7. Average number of packets for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
    df['conn_pkts_for_src_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).mean())

    # 8. Number of times a specific DSTADDRESS has appeared in the last X flow records
    df['conn_dst_adr_count'] = df['Dst IP'].rolling(rolling_window_size).apply(
        lambda x: np.count_nonzero(x == x[rolling_window_size - 1]))

    # 9. Number of distinct destination ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
    df['conn_dst_port_for_dst_adr_count'] = df.groupby('Dst IP')['Dst Port'].apply(
        lambda x: pd.Series(x).rolling(5, min_periods=1).apply(lambda y: len(set(y))))

    # 10. Number of distinct source IPs for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
    df['conn_src_adr_for_dst_adr_count'] = df.groupby('Dst IP')['Src IP'].apply(lambda x: x.replace('.', '')).rolling(
        rolling_window_size).apply(lambda x: len(set(x)))

    # 11. Number of distinct source ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
    df['conn_src_port_for_dst_adr_count'] = df.groupby('Dst IP')['Src Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).apply(lambda y: len(set(y))))

    # 12. Average number of bytes for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
    # TODO: format csv to have bytes

    # 13. Average number of packets for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
    df['conn_pkts_for_dst_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).mean())

    # time-based features
    # 14. Number of flow records in a given time frame
    df['time_flow_count'] = df['Start Time'].rolling(rolling_window_time).count()

    # 15. Number of times a specific SRCADDRESS has appeared within the last X minutes
    df['time_src_adr_count'] = df['Src IP'].rolling(rolling_window_time).apply(
        lambda x: np.count_nonzero(x == x[x.size - 1]))

    # 16. Number of distinct destination ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
    df['time_dst_port_for_src_adr_count'] = df.groupby('Src IP')['Dst Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_time, min_periods=1).apply(lambda y: len(set(y))))

    # 17. Number of distinct destination IPs for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
    df['time_dst_adr_for_src_adr_count'] = df.groupby('Src IP')['Dst IP'].apply(lambda x: x.replace('.', '')).rolling(
        rolling_window_time).apply(lambda x: len(set(x)))

    # 18. Number of distinct source ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
    df['time_src_port_for_src_adr_count'] = df.groupby('Src IP')['Src Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_time, min_periods=1).apply(lambda y: len(set(y))))

    # 19. Average number of bytes for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
    # TODO: format csv to have bytes

    # 20. Average number of packets for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
    df['time_pkts_for_src_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_time, min_periods=1).mean())

    # 21. Number of times a specific DSTADDRESS has appeared within the last X minutes
    df['time_dst_adr_count'] = df['Dst IP'].rolling(rolling_window_time).apply(
        lambda x: np.count_nonzero(x == x[x.size - 1]))

    # 22. Number of distinct destination ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
    df['time_dst_port_for_dst_adr_count'] = df.groupby('Dst IP')['Dst Port'].apply(
        lambda x: pd.Series(x).rolling(5, min_periods=1).apply(lambda y: len(set(y))))

    # 23. Number of distinct source IPs for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
    df['time_src_adr_for_dst_adr_count'] = df.groupby('Dst IP')['Src IP'].apply(lambda x: x.replace('.', '')).rolling(
        rolling_window_time).apply(lambda x: len(set(x)))

    # 24. Number of distinct source ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
    df['time_src_port_for_dst_adr_count'] = df.groupby('Dst IP')['Src Port'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_time, min_periods=1).apply(lambda y: len(set(y))))

    # 25. Average number of bytes for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
    # TODO: format csv to have bytes

    # 26. Average number of packets for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
    df['time_pkts_for_dst_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(
        lambda x: pd.Series(x).rolling(rolling_window_time, min_periods=1).mean())

    # other general features
    # 1. Total flows in the forward direction in the window
    df['conn_fwd_flow_count'] = df['Dst Port'].rolling(rolling_window_size).apply(
        lambda x: ((0 >= x) & (x <= 1023)).sum())
    df['time_fwd_flow_count'] = df['Dst Port'].rolling(rolling_window_time).apply(
        lambda x: ((0 >= x) & (x <= 1023)).sum())

    # 2. Total flows in the backward direction in the window
    df['conn_bwd_flow_count'] = df['Dst Port'].rolling(rolling_window_size).apply(lambda x: (x > 1023).sum())
    df['time_bwd_flow_count'] = df['Dst Port'].rolling(rolling_window_time).apply(lambda x: (x > 1023).sum())

    # 3. Total size of netflows in forward direction in the window

    # 4. Total size of netflows in backward direction in the window

    # 5. Minimum size of flow in forward direction in the window

    # 6. Minimum size of flow in the backward direction in the window

    # 7. Maximum size of flow in forward direction in the window

    # 8. Maximum size of flow in the backward direction in the window

    # 9. Mean size of flow in forward direction in the window

    # 10. Mean size of flow in the backward direction in the window

    # 11. Standard Deviation size of flow in forward direction in the window

    # 12. Standard Deviation size of flow in the backward direction in the window

    # 13. Time between 2 flows in the window in the forward direction

    # 14. Time between 2 flows in the window in the backward direction

    # 15. Number of times a PSH flag was set in flows in the window in the forward direction

    # 16. Number of times a URG flag was set in flows in the window in the forward direction

    # 17. Number of times a PSH flag was set in flows in the window in the backward direction

    # 18. Number of times a URG flag was set in flows in the window in the backward direction

    # 19. Total bytes used in headers in the forward direction in the window

    # 20. Total bytes used in headers in the backward direction in the window

    # 21. Number of flows in the window with FIN flag

    # 22. Number of flows in the window with RST flag

    # 23. Number of flows in the window with FIN flag

    # 24. Number of flows in the window with SYN flag

    # 25. Number of flows in the window with RST flag

    # 26. Number of flows in the window with PUSH flag

    # 27. Number of flows in the window with ACK flag

    # 28. Number of flows in the window with URG flag

    # 29. Number of flows in the window with CWE flag

    # 30. Number of flows in the window with ECE flag
    return df


df = pd.read_csv('../../data/processed/malware-traffic-analysis.net/Malicious/malicious6.csv')
# df['Start Time'] = df.groupby('Start Time').cumcount() * 0.1
# df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Start Time'], unit='s')))
# df.sort_index(inplace=True)
df['Src IP'] = df['Src IP'].apply(lambda x: x.replace('.',''))
df['Dst IP'] = df['Dst IP'].apply(lambda x: x.replace('.',''))


# 3. Number of distinct destination ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
def test(df):
    df['conn_dst_port_for_src_adr_count'] = df.groupby('Src IP')['Dst Port'].apply(lambda x: pd.Series(x).rolling(rolling_window_size, min_periods=1).apply(lambda y: len(set(y))))
    return df

def test2(df):
    df['conn_dst_port_for_src_adr_count2'] = df.groupby('Src IP')['Dst Port'].rolling(rolling_window_size, min_periods=1).apply(lambda y: len(set(y)))
    return df

def wrapper(func, *args, **kwargs):
    def wrapped():
        return func(*args, **kwargs)
    return wrapped

wrapped = wrapper(test, df)
print(timeit.timeit(wrapped, number=2))

wrapped2 = wrapper(test2, df)
print(timeit.timeit(wrapped2, number=2))

# list_of_dfs = []
# chunksize = 1000
# for df in pd.read_csv('../../data/processed/processed_data.csv', chunksize=chunksize):
#     # df = pd.read_csv('../../data/processed/processed_data.csv')
#     df['Start Time'] = df.groupby('Start Time').cumcount() * 0.1
#     df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Start Time'], unit='s')))
#     df.sort_index(inplace=True)
#     df['Src IP'] = df['Src IP'].apply(lambda x: x.replace('.',''))
#     df['Dst IP'] = df['Dst IP'].apply(lambda x: x.replace('.',''))
#     list_of_dfs.append(calculate_features(df))
#
# pd.concat(list_of_dfs)

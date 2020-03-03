import pandas as pd
import numpy as np
rolling_window_size = 5
rolling_window_time = '2s' # in seconds

df = pd.read_csv('../../data/processed/malware-traffic-analysis.net/Malicious/malicious6.csv')
df = df.set_index(pd.DatetimeIndex(pd.to_datetime(df['Start Time'], unit='s')))
df.sort_index(inplace=True)
df['Src IP'] = df['Src IP'].apply(lambda x: x.replace('.',''))
df['Dst IP'] = df['Dst IP'].apply(lambda x: x.replace('.',''))


# connection based features
# 2. Number of times a specific SRCADDRESS has appeared in the last X flow records
df['conn_src_adr_count'] = df['Src IP'].rolling(rolling_window_size).apply(lambda x: np.count_nonzero(x == x[rolling_window_size-1]))

# 3. Number of distinct destination ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
df['conn_dst_port_for_src_adr_count'] = df.groupby('Src IP')['Dst Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_size,min_periods=1).apply(lambda y: len(set(y))))

# 4. Number of distinct destination IPs for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
df['conn_dst_adr_for_src_adr_count'] = df.groupby('Src IP')['Dst IP'].apply(lambda x: x.replace('.','')).rolling(rolling_window_size).apply(lambda x: len(set(x)))

# 5. Number of distinct source ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
df['conn_src_port_for_src_adr_count'] = df.groupby('Src IP')['Src Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_size,min_periods=1).apply(lambda y: len(set(y))))

# 6. Average number of bytes for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
# TODO: format csv to have bytes

# 7. Average number of packets for any of the flow records in which a specific SRCADDRESS has appeared within the last X flow records
df['conn_pkts_for_src_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(lambda x : pd.Series(x).rolling(rolling_window_size,min_periods=1).mean())

# 8. Number of times a specific DSTADDRESS has appeared in the last X flow records
df['conn_dst_adr_count'] = df['Dst IP'].rolling(rolling_window_size).apply(lambda x: np.count_nonzero(x == x[rolling_window_size-1]))

# 9. Number of distinct destination ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
df['conn_dst_port_for_dst_adr_count'] = df.groupby('Dst IP')['Dst Port'].apply(lambda x : pd.Series(x).rolling(5,min_periods=1).apply(lambda y: len(set(y))))

# 10. Number of distinct source IPs for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
df['conn_src_adr_for_dst_adr_count'] = df.groupby('Dst IP')['Src IP'].apply(lambda x: x.replace('.','')).rolling(rolling_window_size).apply(lambda x: len(set(x)))

# 11. Number of distinct source ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
df['conn_src_port_for_dst_adr_count'] = df.groupby('Dst IP')['Src Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_size,min_periods=1).apply(lambda y: len(set(y))))

# 12. Average number of bytes for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
# TODO: format csv to have bytes

# 13. Average number of packets for any of the flow records in which a specific DSTADDRESS has appeared within the last X flow records
df['conn_pkts_for_dst_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(lambda x : pd.Series(x).rolling(rolling_window_size,min_periods=1).mean())


# time-based features
# 14. Number of flow records in a given time frame
df['time_flow_count'] = df['Start Time'].rolling(rolling_window_time).count()

# 15. Number of times a specific SRCADDRESS has appeared within the last X minutes
# df['time_src_adr_count'] = df['Src IP'].rolling(rolling_window_time).count()

# 16. Number of distinct destination ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
df['time_dst_port_for_src_adr_count'] = df.groupby('Src IP')['Dst Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_time,min_periods=1).apply(lambda y: len(set(y))))

# 17. Number of distinct destination IPs for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
df['time_dst_adr_for_src_adr_count'] = df.groupby('Src IP')['Dst IP'].apply(lambda x: x.replace('.','')).rolling(rolling_window_time).apply(lambda x: len(set(x)))

# 18. Number of distinct source ports for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
df['time_src_port_for_src_adr_count'] = df.groupby('Src IP')['Src Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_time,min_periods=1).apply(lambda y: len(set(y))))

# 19. Average number of bytes for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
# TODO: format csv to have bytes

# 20. Average number of packets for any of the flow records in which a specific SRCADDRESS has appeared within the last X minutes
df['time_pkts_for_src_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(lambda x : pd.Series(x).rolling(rolling_window_time,min_periods=1).mean())

# 21. Number of times a specific DSTADDRESS has appeared within the last X minutes

# 22. Number of distinct destination ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
df['time_dst_port_for_dst_adr_count'] = df.groupby('Dst IP')['Dst Port'].apply(lambda x : pd.Series(x).rolling(5,min_periods=1).apply(lambda y: len(set(y))))

# 23. Number of distinct source IPs for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
df['time_src_adr_for_dst_adr_count'] = df.groupby('Dst IP')['Src IP'].apply(lambda x: x.replace('.','')).rolling(rolling_window_time).apply(lambda x: len(set(x)))

# 24. Number of distinct source ports for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
df['time_src_port_for_dst_adr_count'] = df.groupby('Dst IP')['Src Port'].apply(lambda x : pd.Series(x).rolling(rolling_window_time,min_periods=1).apply(lambda y: len(set(y))))

# 25. Average number of bytes for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
# TODO: format csv to have bytes

# 26. Average number of packets for any of the flow records in which a specific DSTADDRESS has appeared within the last X minutes
df['time_pkts_for_dst_adr_avg'] = df.groupby('Src IP')['Tot Pkts'].apply(lambda x : pd.Series(x).rolling(rolling_window_time,min_periods=1).mean())


x = 1;
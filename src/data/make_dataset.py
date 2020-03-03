import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import re
from os import listdir
from os.path import isfile, join
from sklearn.preprocessing import StandardScaler

plt.style.use('ggplot')

def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    stratosphereips_malicous_src_path = "..\..\data\\raw\\stratosphereips\\Malicious\\"
    stratosphereips_malicous_dest_path = "..\..\data\processed\\stratosphereips\Malicious\\"
    process_folder(stratosphereips_malicous_src_path, stratosphereips_malicous_dest_path, 'malicious', 1, 'stratosphereips')

    stratosphereips_normal_src_path = "..\..\data\\raw\\stratosphereips\\Normal\\"
    stratosphereips_normal_dest_path = "..\..\data\processed\\stratosphereips\\Normal\\"
    process_folder(stratosphereips_normal_src_path, stratosphereips_normal_dest_path, 'normal', 0, 'stratosphereips')

    cicFlowMeter_malicous_src_path = "..\..\data\\raw\\malware-traffic-analysis.net\\CICFlowMeter\\"
    cicFlowMeter_malicous_dest_path = "..\..\data\processed\\malware-traffic-analysis.net\\Malicious\\"
    process_folder(cicFlowMeter_malicous_src_path, cicFlowMeter_malicous_dest_path, 'malicious', 1, 'cicFlowMeter')

def process_folder(src_path, dest_path, file_name, malicious, data_source):
    files = [join(src_path, f) for f in listdir(src_path)]
    count = 0
    for f in files:
        try:
            if data_source == 'stratosphereips':
                df = process_stratosphereips_data(f, malicious)
            elif data_source == 'cicFlowMeter':
                df = process_CICFlowMeter_data(f, malicious)
            count += 1
            df.to_csv(dest_path + file_name + str(count) + '.csv', encoding='utf-8', index=False)
        except:
            print('error processing: ' + f)

    print('processed ' + str(count) + ' files')

def process_stratosphereips_data(file_path, malicious):
    df = pd.read_csv(file_path)
    df.rename(columns={'Sport':'SrcPort', 'Dport':'DstPort'}, inplace=True)
    df.dropna(axis=0, inplace=True, subset=['SrcAddr', 'SrcPort', 'DstAddr', 'DstPort', 'TotBytes', 'SrcBytes', 'TotPkts'])
    df['Label'] = malicious
    df['Dur'] = df['Dur'] * 1000
    df['StartTime'] = df['StartTime'].map(convert_to_timestamp) + df.groupby('StartTime').cumcount() * 0.1
    df['Endtime'] = df['StartTime'] + df['Dur']
    # df.drop(['Dir', 'srcUdata', 'dstUdata'], axis=1, inplace=True)
    df.drop(['Dir'], axis=1, inplace=True)
    # print(df.iloc[[5]])
    return df

def process_CICFlowMeter_data(file_path, malicious):
    df = pd.read_csv(file_path)
    df = df[['Timestamp', 'Flow Duration', 'Protocol', 'Src IP', 'Src Port', 'Dst IP', 'Dst Port', 'Tot Fwd Pkts','Tot Bwd Pkts','TotLen Fwd Pkts','TotLen Bwd Pkts', 'Label']]
    df.rename(columns={'Timestamp':'Start Time', 'Flow Duration':'Duration'}, inplace=True)
    df.dropna(axis=0, inplace=True, subset=['Src IP', 'Src Port', 'Dst IP', 'Dst Port', 'Tot Fwd Pkts','Tot Bwd Pkts','TotLen Fwd Pkts','TotLen Bwd Pkts'])
    df['Label'] = malicious
    df['Duration'] = df['Duration'] / 1000
    df['Start Time'] = df['Start Time'].map(convert_CICFlowMeter_timestamp) + df.groupby('Start Time').cumcount() * 0.1
    df['End Time'] = df['Start Time'] + df['Duration']
    df['Tot Pkts'] = df['Tot Fwd Pkts'] + df['Tot Bwd Pkts']
    return df

def convert_to_timestamp(value):
    date_search = re.search('(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2}).(\d{6})', value)
    date = datetime(int(date_search.group(1)), int(date_search.group(2)), int(date_search.group(3)), int(date_search.group(4)), int(date_search.group(5)), int(date_search.group(6)), int(date_search.group(7)))
    return datetime.timestamp(date)

def convert_CICFlowMeter_timestamp(value):
    date_search = re.search('(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})\s+(AM|PM)', value)
    hour = date_search.group(4)
    if hour == '12':
        hour = 0
    elif date_search.group(7) == 'PM':
        hour = int(hour) + 12
    date = datetime(int(date_search.group(3)), int(date_search.group(2)), int(date_search.group(1)), hour, int(date_search.group(5)), int(date_search.group(6)))
    return datetime.timestamp(date)

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

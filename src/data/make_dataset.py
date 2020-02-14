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

plt.style.use('ggplot')


def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')

    malicous_src_path = "..\..\data\\raw\stratosphereips\Malicious\\"
    malicous_dest_path = "..\..\data\processed\Malicious\\"
    process_folder(malicous_src_path, malicous_dest_path, 'malicious', 1)
    normal_src_path = "..\..\data\\raw\stratosphereips\\Normal\\"
    normal_dest_path = "..\..\data\processed\\Normal\\"
    process_folder(normal_src_path, normal_dest_path, 'normal', 0)

def process_folder(src_path, dest_path, file_name, malicious):
    files = [join(src_path, f) for f in listdir(src_path)]
    count = 0
    for f in files:
        try:
            df = process_data(f, malicious)
            count += 1
            df.to_csv(dest_path + file_name + str(count) + '.csv', encoding='utf-8', index=False)
        except:
            print('error processing: ' + f)

    print('processed ' + str(count) + ' files')

def process_data(file_path, malicious):
    df = pd.read_csv(file_path)
    df.dropna(axis=0, inplace=True, subset=['SrcAddr', 'Sport', 'DstAddr', 'Dport', 'TotBytes', 'SrcBytes', 'TotPkts'])
    df['Label'] = malicious
    df['StartTime'] = df['StartTime'].map(convert_to_timestamp)
    df['Endtime'] = df['StartTime'] + (df['Dur'] * 1000)
    df.drop(['Dir'], axis=1, inplace=True)
    df.rename(columns={'Sport':'SrcPort', 'Dport':'DPort'}, inplace=True)
    # print(df.iloc[[5]])
    return df


def convert_to_timestamp(value):
    date_search = re.search('(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2}).(\d{6})', value)
    date = datetime(int(date_search.group(1)), int(date_search.group(2)), int(date_search.group(3)), int(date_search.group(4)), int(date_search.group(5)), int(date_search.group(6)), int(date_search.group(7)))
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

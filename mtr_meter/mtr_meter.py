import shlex
import logging
import time
import argparse
import subprocess
import pandas as pd
from bokeh import plotting as bkp
from io import StringIO


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(module)s/%(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)


class FailedResolveHost(Exception):
    pass


class TemporaryFailureResolveHost(Exception):
    pass


def run_mtr(host: str, n_measurements: int):
    logger.info("Running data collection for host {}".format(host))
    mtr_cmd = shlex.split('mtr -C -c {} {}'.format(n_measurements, host))
    response = subprocess.run(mtr_cmd, capture_output=True, timeout=360, check=True, encoding='utf-8')
    if response.stderr.strip() == 'Failed to resolve host: Name or service not known':
        raise FailedResolveHost
    if response.stderr.strip() == 'Failed to resolve host: Temporary failure in name resolution':
        raise TemporaryFailureResolveHost
    return response


def parse_mtr_response(mtr_response: subprocess.CompletedProcess):
    df_mtr = pd.read_csv(StringIO(mtr_response.stdout))
    df_mtr.drop(['Mtr_Version', 'Unnamed: 14'], axis=1, inplace=True)
    df_mtr.rename(columns={' ':'dropped' }, inplace=True)
    df_mtr.columns = [col.lower() for col in df_mtr.columns]
    df_mtr['start_time'] = pd.to_datetime(df_mtr.start_time, unit='s')
    return df_mtr


def run_measurement(host: str, n_measurements: int, retries: int = 100, sleep_time: int = 300):
    df_result = pd.DataFrame()

    while retries > 0:
        try:
            mtr_response = run_mtr(host, n_measurements)
            df_result  = df_result.append(parse_mtr_response(mtr_response))
            time.sleep(sleep_time)
        except TemporaryFailureResolveHost:
            logger.warning('No connection, will retry {} times'.format(retries))
            time.sleep(sleep_time)
            retries = retries - 1
            continue
        except FailedResolveHost:
            logger.error("Host '{}' not found".format(host))
            return df_result
        except KeyboardInterrupt:
            break

    return df_result

def create_graph(df):
    p = bkp.figure()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", '--host')
    parser.add_argument('-n', '--n_measurements')
    parser.add_argument('-r', '--retries', type = int)
    args = parser.parse_args()
    df= run_measurement(args.host, args.n_measurements, args.retries)
    df.groupby('start_time').last().to_csv('mtr_meter.csv', encoding='utf-8', sep=';')
    #print(df_lasthop)

import shlex
import logging
import subprocess
import pandas as pd
from io import StringIO


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(module)s/%(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)


def run_mtr(n_measurements, host):
    """Run mtr command

    n_measurements
        Number of measurements for mtr to run
    host
        Host to run the measurements
    return
        subprocess.CompletedProcess instance
    """
    mtr_cmd = shlex.split('mtr -C -c {} {}'.format(n_measurements, host))
    return subprocess.run(mtr_cmd, capture_output=True, timeout=360, check=True, encoding='utf-8')


def parse_mtr_response(mtr_response: subprocess.CompletedProcess):
    if mtr_response.stderr.strip() == 'Failed to resolve host: Name or service not known':
        raise FileNotFoundError
    df_mtr = pd.read_csv(StringIO(mtr_response.stdout))
    df_mtr.drop(['Mtr_Version', 'Unnamed: 14'], axis=1, inplace=True)
    df_mtr.rename(columns={' ':'dropped' }, inplace=True)
    df_mtr.columns = [col.lower() for col in df_mtr.columns]
    df_mtr['start_time'] = pd.to_datetime(df_mtr.start_time, unit='s')
    return df_mtr


def measure_mtr(host: str, n_measurements: int = 10):
    """Do n mtr measurements on host.

    Parameters
    ----------
    host
        The host used to measure the latency. It can be an URL or IP address

    n_measurements
        The number of measurements that mtr do before registering the result. Default is 10.

    Return
    ------
        Returns a Pandas data frame with the results
    """    

    try:
        logger.info("Running data collection for host {}".format(host))
        response = run_mtr(n_measurements, host)
        df_parsed = parse_mtr_response(response)
        return df_parsed
    except subprocess.TimeoutExpired:
        logger.error("Timeout")
    except subprocess.CalledProcessError:
        logger.error("Process exited with non-zero code")
    except subprocess.SubprocessError:
        logger.error("Error in the mtr command")
    except FileNotFoundError:
        logger.error("Could not find mtr command or host '{}' invalid".format(host))



if __name__ == "__main__":
    df = pd.DataFrame()

    for i in range(3):
        df = df.append(measure_mtr('terra.com.br', 2))
    print(df.groupby('start_time').last())

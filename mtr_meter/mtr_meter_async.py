import shlex
import logging
import asyncio
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


def parse_mtr_response(mtr_response: str):
    df_mtr = pd.read_csv(StringIO(mtr_response))
    df_mtr.drop(['Mtr_Version', 'Unnamed: 14'], axis=1, inplace=True)
    df_mtr.rename(columns={' ':'dropped' }, inplace=True)
    df_mtr.columns = [col.lower() for col in df_mtr.columns]
    df_mtr['start_time'] = pd.to_datetime(df_mtr.start_time, unit='s')
    return df_mtr

async def run_mtr_async(host: str, n_measurements: int):

    mtr_cmd = 'mtr -C -c {} {}'.format(n_measurements, host)
    proc = await asyncio.create_subprocess_shell(
        mtr_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    await asyncio.sleep(2)

    stdout, stderr = await proc.communicate()
    df = parse_mtr_response(stdout.decode())
    return df


if __name__ == "__main__":
    df = asyncio.run(run_mtr_async('terra.com.br', 2))
    print(df)

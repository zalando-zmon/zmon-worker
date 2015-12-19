import os
import subprocess


def main():

    # If running on AWS, fetch the account number
    # TODO: fix this shell code and move it somewhere sane
    try:
        worker_account = subprocess.check_output('curl --connect-timeout 5 --silent http://169.254.169.254/latest/meta-data/iam/info/ | grep "ProfileArn" | grep -E -o "iam::([0-9]+)" | grep -E -o "[0-9]+"', shell=True)[:-1]
        os.environ['WORKER_ACCOUNT'] = 'aws:' + worker_account
    except:
        os.environ['WORKER_ACCOUNT'] = 'aws:error-during-startup'

    redis_keys = set(['broker', 'backend', 'redis_servers'])
    redis_host = os.environ.get("WORKER_REDIS_HOST", 'localhost')
    redis_port = os.environ.get("WORKER_REDIS_PORT", '6379')

    for k in redis_keys:
        os.environ['WORKER_{}'.format(k.upper())] = 'redis://{}:{}/0'.format(k, redis_host, redis_port)

    subprocess.check_output(["zmon-worker", "-c", "/app/config.yaml"])

if __name__ == "__main__":
    main()

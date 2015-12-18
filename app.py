import os
import subprocess


def main():

    conf = open('/app/web.conf', 'ab')

    # WRITE ENV VARS TO CONFIG FILE
    # TODO: we should get rid of the old CherryPy config file..
    for k, v in os.environ.items():
        if k.startswith('WORKER_'):
            conf.write(k.replace("WORKER_", "").replace("_", ".").lower() + " = '" + v + "'\n")

    # If running on AWS, fetch the account number
    try:
        worker_account = subprocess.check_output('curl --connect-timeout 5 --silent http://169.254.169.254/latest/meta-data/iam/info/ | grep "ProfileArn" | grep -E -o "iam::([0-9]+)" | grep -E -o "[0-9]+"', shell=True)[:-1]
        conf.write("account='aws:"+worker_account+"'\n")
    except:
        conf.write("account='aws:error-during-startup'\n")

    redis_keys = set(['broker', 'backend', 'redis_servers'])
    redis_host = os.environ.get("WORKER_REDIS_HOST", 'localhost')
    redis_port = os.environ.get("WORKER_REDIS_PORT", '6379')

    for k in redis_keys:
        conf.write("{} = 'redis://{}:{}/0'\n".format(k, redis_host, redis_port))

    conf.close()

    subprocess.check_output(["zmon-worker", "-c", "/app/web.conf"])

if __name__ == "__main__":
    main()

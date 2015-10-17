import os
import subprocess

# WRITE ENV VARS TO CONFIG FILE

def main():

    env_keys = set(["WORKER_NOTIFICATION_SMS_APIKEY",
                   "WORKER_MYSQL_USER",
                   "WORKER_MYSQL_PASSWORD",
                   "SCALYR_READ_KEY",
                   "WORKER_POSTGRESQL_USER",
                   "WORKER_POSTGRESQL_PASSWORD",
                   "WORKER_ACCOUNT",
                   "WORKER_TEAM"])

    conf = open('/app/web.conf', 'ab')

    for k in env_keys:
        v = os.environ.get(k, None)
        if v is not None:
            conf.write(k.replace("WORKER_", "").replace("_",".").lower()+" = " + v + "\n")

    redis_keys = set(['broker', 'backend', 'redis_servers'])
    redis_host = os.environ.get("WORKER_REDIS_HOST", 'localhost')
    redis_port = os.environ.get("WORKER_REDIS_PORT", '6379')

    for k in redis_keys:
        conf.write("{} = 'redis://{}:{}/0'\n".format(k, redis_host, redis_port))

    conf.close()

    subprocess.check_output(["zmon-worker", "-c", "/app/web.conf"])

if __name__ == "__main__":
    main()
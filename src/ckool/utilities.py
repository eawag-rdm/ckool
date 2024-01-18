import os
import sys
from subprocess import PIPE, CalledProcessError, run


def get_secret(name):
    """first checking the 'environment variables' for the secret then checking 'pass' the password manager."""
    secret = os.environ.get(name)
    if secret is not None:
        return secret

    try:
        proc = run(["pass", name], stdout=PIPE, stderr=PIPE)
        proc.check_returncode()
    except CalledProcessError:
        sys.exit("ERROR: Didn't find {} in .password-store either")
    else:
        secret = proc.stdout.decode("utf-8").strip("\n")
        return secret

import os


def chk_path(path):
    if not os.path.exists(path):
        os.mkdir(path)
    return path


import hashlib


def get_md5(strr):
    strr = strr.encode('utf-8')
    m = hashlib.md5()
    m.update(strr)
    return str(m.hexdigest())

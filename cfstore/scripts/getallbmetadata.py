from youtube_dl import main
import cfparse_file
import os
from cfstore.config import CFSconfig

if __name__ = '__main__':
    state = CFSconfig()
    for filename in os.listdir(os.curdir):
        cfparse_file(state.db,filename)
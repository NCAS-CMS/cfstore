import paramiko
import posixpath
from stat import S_ISREG, S_ISDIR
import time

class SSHLight:
    """
    Provides a lightweight interface to some paramiko and pysftp like
    functionality, *assuming there is a running ssh agent on the client
    machine*.
    """

    def __init__(self, host, username, port=22, logging=True):
        """
        Initialise with the target host and username, and optionally port.
        Because we love jasmin, we include a couple of short form host
        names for jasmin, so you can use host = xfer1|xfer3.
        """
        jasmin_hosts = {
            'xfer1':'xfer1.jasmin.ac.uk',
            'xfer3':'xfer3.jasmin.ac.uk',
        }
        if host in jasmin_hosts:
            host = jasmin_hosts[host]

        self.logging = logging
        if self.logging:
            paramiko.util.log_to_file('paramiko.log')

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(host, port, username)
        self.transport = client.get_transport()
        self._sftp = paramiko.SFTPClient.from_transport(self.transport)

    def get(self, remotepath, localpath):
       """
       Get remote_path and store it in local_path
       """
       self._sftp.get(remotepath, localpath)

    def walktree(self, remotepath, fcallback, dcallback=None, ucallback=None,
                 recurse=True):
        """
        Recursively descend, depth first, the directory tree rooted at
        remotepath, calling discreet callback functions for each regular file,
        directory and unknown file type. (This is a direct clone of the pysftp
        function.)

        :param str remotepath:
            root of remote directory to descend, use '.' to start at
            :attr:`.pwd`
        :param callable fcallback:
            callback function to invoke for a regular file.
            (form: ``func(str)``)
        :param callable dcallback:
            callback function to invoke for a directory. (form: ``func(str)``)
        :param callable ucallback:
            callback function to invoke for an unknown file type.
            (form: ``func(str)``)
        :param bool recurse: *Default: True* - should it recurse

        :returns: None
        """

        for entry in self._sftp.listdir(remotepath):
            pathname = posixpath.join(remotepath, entry)
            mode = self._sftp.stat(pathname).st_mode
            if S_ISDIR(mode):
                # It's a directory, call the dcallback function
                if dcallback is not None:
                    dcallback(pathname)
                if recurse:
                    # now, recurse into it
                    self.walktree(pathname, fcallback, dcallback, ucallback)
            elif S_ISREG(mode):
                # It's a file, call the fcallback function
                fcallback(pathname)
            else:
                # Unknown file type
                if ucallback is not None:
                    ucallback(pathname)

    def get_size(self, remote_path):
        """
        Get the size in bytes of remote file object at remote_path
        """
        return self._sftp.stat(remote_path).st_size

    def get_files_and_sizes(self, remotepath, subcollections=False):
        """
        Get a list of all files and their sizes found in the
        directories which live below remote-path.

        If subcollections (default False, and not yet implemented),
        additionally return for each directory below that path, a
        lists of files for that directory (without further recursion
        below each of those sub-directories).

        """

        if subcollections:
            raise NotImplementedError

        files = []

        def callback(file):
            files.append((file, self.get_size(file)))

        if self.logging:
            stime = time.time()

        self.walktree(remotepath, callback)

        if self.logging:
            etime = time.time()
            print(f'Walking {remotepath} for {len(files)} files took {etime-stime:.2f}s')

        return files


if __name__ == "__main__":

    s = SSHLight('xfer1','lawrence')
    flist = s.get_files_and_sizes('hiresgw/xjanp')
    print(flist)


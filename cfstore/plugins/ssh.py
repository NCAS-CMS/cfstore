from distutils.log import error
from inspect import walktree
import paramiko
import posixpath, os, glob, fnmatch
from itertools import product
from stat import S_ISREG, S_ISDIR
import time
import json

from cfstore.cfparse_file import cfparse_file


class SSHcore:
    """ Provides a lightweight setup for establishing some SSH
    tunnelling to a remote host etc.
    """

    def __init__(self, host, username, port=22, logging=True):
        """
        Initialise with the target host and username, and optionally port.
        Because we love jasmin, we include a couple of short form host
        names for jasmin, so you can use host = xfer1|xfer3.
        """
        jasmin_hosts = {
            'xfer1': 'xfer1.jasmin.ac.uk',
            'xfer3': 'xfer3.jasmin.ac.uk',
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
        self._client = client


class SSHlite(SSHcore):
    """
    Provides a lightweight interface to some paramiko and pysftp like
    functionality, *assuming there is a running ssh agent on the client
    machine*.
    """

    def isalive(self):
        return self.transport.is_active()

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

        try:
            self.walktree(remotepath, callback)
        except FileNotFoundError:
            raise FileNotFoundError(f' check {remotepath} exists?')


        if self.logging:
            etime = time.time()
            print(f'Walking {remotepath} for {len(files)} files took {etime-stime:.2f}s')

        return files

    def get_b_metadata(self, remotepath,db):
        print(f'SSH is getting B metadata')
        
        parselambda = lambda file: self.sshparse(db,file)
        self.walktree(remotepath, parselambda) 

    def sshparse(self,db,file):
        remotefile = self._sftp.open(file)
        cfparse_file(db,remotefile)

    def run_script(self, remotepath, collection, script):
        scriptname = os.path.basename(script)
        print("Running script:",scriptname)
        print("Putting script")
        print("Putting script ",scriptname,"from", script ,"to", remotepath)
        remotescript = remotepath +scriptname
        try:
            self._sftp.put(script,remotescript)
        except:
            print("Failed to put script on remote server")
        print("Moving to filelocation")
        print("Moving to filelocation",remotepath)
        try:
            self._client.exec_command('cd '+remotepath)
        except:
            print("Could not move to remote directory")
            print("Removing script from remote server")
            self._sftp.remove(remotescript)
        stdin, stdout, stderr=self._client.exec_command('pwd')
        for line in stderr:
            print("err:",line)
        for line in stdout:
            print("lsout:",line)
        print("Executing script")
        print('Executing \"python '+scriptname+"\"")
        try:
#            stdin, stdout, stderr = self._client.exec_command('ls')
            stdin, stdout, stderr = self._client.exec_command('python '+remotepath+scriptname)
            print("Script executed")
        except:
            print("Could not successfully execute script")
            print("Removing script from remote server")
            self._sftp.remove(remotescript)
        print(stderr)
        for line in stderr:
            print("err:",line)
        for line in stdout:
            print("out:",line)
        with open("sample.json","w") as writepath:
            json.dump(stdout,writepath) 
        self._sftp.remove(remotescript)

    def globish(self, remotepath, expression):
        """
        Approximate match for globbing  <expression> against information from <remotepath> remote directory.
        Without recursion!
        """
        paths = self._sftp.listdir(remotepath)
        return find_matching_paths(paths, expression)


def find_matching_paths(pathlist, pattern):
    """
    Given a list of paths, return a list of those paths which match
    the input pattern.  The pattern should be expressed using the
    Python glob pattern matching syntax for a unix file system.

    """

    def _in_trie(trie, pth):
        """Determine if path is completely in trie"""
        curr = trie
        for e in pth:
            try:
                curr = curr[e]
            except KeyError:
                return False
        return None in curr

    if os.altsep:  # normalise
        pattern = pattern.replace(os.altsep, os.sep)
    pattern = pattern.split(os.sep)

    # build a trie out of path elements; efficiently search on prefixes
    path_trie = {}
    for path in pathlist:
        if os.altsep:  # normalise
            path = path.replace(os.altsep, os.sep)
        _, path = os.path.splitdrive(path)
        elems = path.split(os.sep)
        current = path_trie
        for elem in elems:
            current = current.setdefault(elem, {})
        current.setdefault(None, None)  # sentinel

    matching = []

    current_level = [path_trie]
    for subpattern in pattern:
        if not glob.has_magic(subpattern):
            # plain element, element must be in the trie or there are 0 matches
            if not any(subpattern in d for d in current_level):
                return []
            matching.append([subpattern])
            current_level = [d[subpattern] for d in current_level if subpattern in d]
        else:
            # match all next levels in the trie that match the pattern
            matched_names = fnmatch.filter({k for d in current_level for k in d}, subpattern)
            if not matched_names:
                # nothing found
                return []
            matching.append(matched_names)
            current_level = [d[n] for d in current_level for n in d.keys() & set(matched_names)]

    return [os.sep.join(p) for p in product(*matching)
            if _in_trie(path_trie, p)]


class SSHTape(SSHcore):

    def get_html(self, url, option='curl'):
        """
        This is a temporary crufty method of getting html from a webserver via
        a request running on the remote SSH server. Assume curl is available
        otherwise pass wget or whatever else might be available.
        """
        # note the importance of the single quotes, otherwise curl stops looking at the & sign in the url.
        stdin, stdout, stderr = self._client.exec_command(f"{option} '{url}'")
        error = stderr.readlines()
        if error:
            # probably should do some more sophisticated error handling.
            if "Could not resolve host" in error[-1]:
                err = error[-1].find("curl")
                raise ValueError(error[-1][err:])
        # look out, if I do this with google.com I get a unicode error of some sort ...  and I couldn't
        # work out how to fixit ... just joining on u"" didn't do it ..
        lines = stdout.readlines()
        return "".join(lines)


if __name__ == "__main__":

    s = SSHlite('xfer1', 'lawrence')

    dlist = s.globish('hiresgw', 'xj*')
    print(dlist)

    dlist = s.globish('hiresgw', 'xj???')
    print(dlist)

    flist = s.get_files_and_sizes('hiresgw/xjanp')
    print(flist)

    s = SSHTape('xfer1', 'lawrence')

    html = s.get_html('http://et-monitor.fds.rl.ac.uk/et_user/')
    print(f"**\n{html}\n**")


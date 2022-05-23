#
import unittest
from bs4 import BeautifulSoup
from urllib.request import urlopen
import pickle, glob, os
from dateutil.parser import parse as dateparse
from datetime import datetime
from cfstore.plugins.ssh import SSHTape

WEBSITE = "http://et-monitor.fds.rl.ac.uk/et_user/"


class Souper:
    """
    Support two routes to getting BeautifulSoup from
    html content - directly from an accessible web
    server, or indirectly, via ssh to a host which
    has access to that content.
    """
    def __init__(self, ssh_host, ssh_user):
        if ssh_host is None:
            self.ssh = None
        else:
            self.ssh = SSHTape(ssh_host, ssh_user)

    def get(self, url):
        """
        Return html content
        """
        target = WEBSITE + url
        if self.ssh is None:
            html = urlopen(target)
        else:
            html = self.ssh.get_html(target)
        return BeautifulSoup(html, features='html.parser')


class ET_Workspace:
    """
    Collect all file information about a specific workspace.
    This version assumes all data loaded is still present.
    """
    def __init__(self, workspace_name, ssh_host=None, ssh_user=None):
        """
        Initialise with the GWS/ET workspace, and if necessary, ssh
        credentials to access remotely.
        """
        self.name = workspace_name
        self.batches = {}
        self.file_count = 0
        self.quota_allocated = 0
        self.quota_used = 0
        self.volume = 0
        self.souper = Souper(ssh_host, ssh_user)
        if self.souper.ssh is None:
            print('Elastic tape interface being used in "Inside RAL" mode')
        self.load_from_et()

    def load_from_et(self):
        url = f"ET_Holdings_Summary.php?workspace={self.name}&level=top"
        print(url)
        soup = self.souper.get(url)
        summary_td = soup.find_all('table')[1].find_all('tr')[1].find_all('td')
        self.file_count = summary_td[0]

        # quotas in bytes
        self.quota_allocated = int(summary_td[4].text)
        self.quota_used = int(summary_td[5].text)
        self.volume = int(summary_td[6].text)
        url = f"ET_Holdings_Summary.php?workspace={self.name}&level=batches"
        soup = self.souper.get(url)
        try:
            rows = soup.find_all('table')[2].find_all('tr')
        except:
            rows = []
        self.batches = {B.name: B for B in [Batch(r, self) for r in rows[1:]]}
        print(f'{self.name}: {len(self.batches)} batches')

    def __str__(self):
        return self.name

    def info(self):
        return f'{self.name}: {len(self.batches)} batches, {self.file_count} files'


class Batch:
    """
    Describe a batch and the files within.
    """
    def __init__(self, row, workspace):
        """ Initialise with a soup row element from
            http://et-monitor.fds.rl.ac.uk/et_user/
            ET_Holdings_Summary.php?workspace=X&level=batches
        """
        self.workspace = workspace
        link = row.find('a')
        self.name = link.text.split(' ')[-1]
        td = row.find_all('td')
        self.creation_time = td[3].text
        self.file_count = int(td[4].text)
        self.file_address = td[4].url
        self.batch_size_bytes = int(td[5].text)
        self.url = link['href']
        self.transfers = []
        file_url = 'ET_Batch_Input_File_Details.php?batch='+self.name
        soup = self.workspace.souper.get(file_url)
        tables = soup.find_all('table')
        file_rows = tables[1].find_all('tr')[1:]
        file_data = [r.find_all('td') for r in file_rows]
        self.files = {f[0].text: int(f[1].text) for f in file_data}

    def load_transfers(self):
        soup = self.workspace.souper.get(self.url)
        tables = soup.find_all('table')
        self.transfers = [TransferSummary(r) for r in self.tables[3].find_all('tr')[1:]]


class TransferSummary:
    """ Define an ET Transfer Summary"""
    def __init__(self, row):
        """ Initialise with soup row element from the third table on
            http://et-monitor.fds.rl.ac.uk/et_user/
            ET_Batch_Input_Summary.php?workspace=X&caller=etjasmin&batch=Y
        """
        link = row.findf('a')
        self.name = link.text
        td = row.find_all('td')
        self.status = td[1].text
        self.creation_time = td[2].text
        self.castor_time = td[3].text
        self.file_count = int(td[4].text)
        self.size_Gb = float(td[5].text)
        self.checksum = td[6].text

    def __str__(self):
        return f'{self.name} (created {self.creation_time}, nfiles {self.file_count}, size {self.size_Gb})'


class File:
    """
    User files, allow attributes to vary on initialisation by keyword
    """
    def __init__(self, **kwargs):
        for kw in kwargs:
            setattr(self, kw, kwargs[kw])


class Transition:
    """
    A transition in state during the ET ingestion process
    """
    def __init__(self, initial, final, start_time, end_time):
        self.initial, self.host_start = tuple(initial.split(':'))
        self.final, self.host_end = tuple(final.split(':'))
        self.start_time = dateparse(start_time)
        self.end_time = dateparse(end_time)

    @property
    def duration(self):
        return (self.end_time - self.start_time).total_seconds()

    def __str__(self):
        return f'{self.initial}->{self.final} at {self.end_time.isoformat()} (duration {self.duration}).'


class Transfer:
    # Unclear whether this is currently used. There will be a problem with using getsoup
    # unless inside the firewall.
    """
    Holds full details of a transfer from
            http://et-monitor.fds.rl.ac.uk/et_user/
            ET_Aggregation_File_Details.php?aggregation=235228
    """
    def __init__(self, name):

        self.name = name
        url = f'ET_Aggregation_File_Details.php?aggregation={name}'
        souper = Souper(None, None)
        soup = souper.get(url)
        self.files = []
        self.aggregation_name = soup.find('p').text.split()[-1]

        self.transitions = []
        # truncate to integer seconds since later times don't have fractions, results in negatives!
        summary_td = soup.find('table').find_all('tr')[1].find_all('td')
        started = summary_td[-3].text.split('.')[0]
        self.size = float(summary_td[2].text)
        self.checksum = summary_td[-2].text
        self.castor_time = dateparse(summary_td[-1].text)

        history_table = soup.find(id='Aggregation History')
        for trow in history_table.find_all('tr')[1:]:
            initial, final, ended = tuple([v.text for v in trow.find_all('td')])
            self.transitions.append(Transition(initial, final, started, ended))
            started = ended
            if self.transitions[-1].duration < 0:
                print('Examine ', self.name)
                for t in self.transitions:
                    print(t)
                exit(1)

        # current_tape version of this page has two tables with the same id
        file_table = soup.find_all('table', id='Aggregation details')[1]
        for f in file_table.find_all('tr')[1:]:
            td = f.find_all('td')
            self.files.append(File(name=td[0].findf('a').text, size=int(td[1].text)))

    def __str__(self):
        return '\n'.join([str(t) for t in self.transitions])


class TestBatch(unittest.TestCase):

    def test_etworkspace(self):
        """
        Test we can see hiresgw without breaking
        """
        w = ET_Workspace('hiresgw', 'xfer1', 'lawrence')


if __name__ == "__main__":
    unittest.main()



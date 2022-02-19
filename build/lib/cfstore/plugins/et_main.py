from cfstore.plugins.et_utils import ET_Workspace
import os


def _load_et(workspace='hiresgw'):
    """
    Load a workspace from elastic tape
    """
    et = ET_Workspace(workspace)
    return et


def parse_workspace_into_db(etw, db):
    """
    For a given workspace, etw, being an instance of an ET_Workspace,
    parse it into a CollectionDB instance inside db.
    """

    for b in etw.batches:
        batch = etw.batches[b]
        cname = 'et_'+batch.name
        db.create_collection(cname, 'None', {})
        files = []
        for f in batch.files:
            path, name = os.path.split(f)
            size = batch.files[f]
            files.append({'path': path, 'name': name, 'size': size})
        db.upload_files_to_collection('elastic_tape', cname, files)


def et_main(db, operation, gws):
    """
    Carry out <operation> on the <gws> group workspace
    using the <db> instance provided.
    """
    db.create_location('elastic_tape')
    if operation == 'init':
        etw = _load_et(workspace=gws)
        parse_workspace_into_db(etw, db)

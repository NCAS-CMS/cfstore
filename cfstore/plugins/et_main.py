from cfstore.plugins.et_utils import ET_Workspace


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
        db.create_collection(batch.name, 'None', {})
        files = [(k, batch.files[k],) for k in batch.files]
        db.upload_files_to_collection('et_'+batch.name, files)


def et_main(db, operation, gws):
    """
    Carry out <operation> on the <gws> group workspace
    using the <db> instance provided.
    """
    etw = _load_et()
    parse_workspace_into_db(etw, db)

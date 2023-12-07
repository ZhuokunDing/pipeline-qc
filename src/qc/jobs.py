import pandas as pd
from qc import virtual as V
import numpy as np

jobs_schemas = {
    "treadmill": V.treadmill,
    "pupil": V.pupil,
    "meso": V.meso,
    "reso": V.reso,
    "fuse": V.fuse,
    "experiment": V.experiment,
    "collection": V.collection,
    "stimulus": V.stimulus,
}

def rec_to_dict(rec):
    if isinstance(rec, dict):
        return rec
    elif len(rec) == 1:
        return {name:rec[0][name] for name in rec.dtype.names}
    else:
        return [{name:r[name] for name in rec.dtype.names} for r in rec]

def compatible_keys(key1, key2):
    '''
    Check if:
    1. key1 and key2 have overlapping fields
    2. key1 and key2 have the same values for overlapping fields
    '''
    # 1. key1 and key2 have overlapping fields
    if not set(key1.keys()) & set(key2.keys()):
        return False
    # 2. key1 and key2 have the same values for overlapping fields
    for k in set(key1.keys()) & set(key2.keys()):
        if key1[k] != key2[k]:
            return False
    return True

def get_jobs(schemas, target_keys):
    dfs = []
    for schema_name, schema in schemas.items():
        df = schema.schema.jobs.fetch(format='frame').reset_index()
        df['key'] = df['key'].apply(rec_to_dict)
        df['schema'] = schema
        dfs += [df]
    dfs = pd.concat(dfs)
    dfs['key_summary'] = dfs['key'].apply(lambda key : '-'.join([str(v) for v in key.values()]))
    target = np.zeros(len(dfs), dtype=bool)
    for target_key in target_keys:
        target = target | dfs['key'].apply(lambda key : compatible_keys(key, target_key))
    return dfs.loc[target]

def restrict_with_jobs_df(jobs_df, index):
    return jobs_df.loc[index, 'schema'].schema.jobs & dict(jobs_df.loc[index, ['table_name', 'key_hash']])

def delete_errors(
    jobs_df, 
    errors=(
            'LostConnectionError: Connection was lost during a transaction.',
            "OperationalError: (1205, 'Lock wait timeout exceeded; try restarting transaction')",
        ),
):
    for i, job in jobs_df.iterrows():
        if job['status'] == 'error':
            job_key = {'key_hash':job['key_hash'], 'table_name':job['table_name']}
            job_info = (job['schema'].schema.jobs & job_key).fetch1()
            if errors=='all':
                (job['schema'].schema.jobs & job_key).delete()
            else:
                if job_info['error_message'] in errors:
                    (job['schema'].schema.jobs & job_key).delete()

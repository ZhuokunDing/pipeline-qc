from . import scan, logger
import pandas as pd

class Batch:

    def __init__(self, scan_keys) -> None:
        self.scans = [
            scan.Scan(
                animal_id=key["animal_id"],
                session=key["session"],
                scan_idx=key["scan_idx"],
            )
            for key in scan_keys
        ]

    @property
    def keys(self):
        return [s.key for s in self.scans]
    
    @property
    def stacks(self):
        return [s.stack for s in self.scans]
    
    @property
    def stack_regs(self):
        return [s.stack_reg for s in self.scans]

    def delete_errors(self, errors=None):
        for s in self.scans:
            if errors is None:
                s.delete_errors()
            else:
                s.delete_errors(errors)

    def fill_registration_task(self, force=False):
        for s in self.scans:
            logger.info(f"Inserting registration task for {s.key}")
            try:
                s.fill_registration_task(force=force)
            except Exception as e:
                logger.error(f"Failed to insert registration task for {s.key}: {e}")

    def fill_rot_task(self, force=False):
        for s in self.scans:
            logger.info(f"Inserting RegistrationOverTime task for {s.key}")
            try:
                s.fill_rot_task(force=force)
            except Exception as e:
                logger.error(f"Failed to insert RegistrationOverTime task for {s.key}: {e}")

    @property 
    def jobs_df(self):
        dfs = []
        for s in self.scans:
            df = s.jobs_df
            for k in s.key:
                df[k] = s.key[k]
            dfs.append(df)
        return pd.concat(dfs).set_index(['animal_id', 'session', 'scan_idx'])
    
    @property
    def stack_jobs_df(self):
        dfs = []
        for s in self.scans:
            df = s.stack_jobs_df
            for k in s.key:
                df[k] = s.key[k]
            dfs.append(df)
        return pd.concat(dfs).set_index(['animal_id', 'session', 'scan_idx'])
    
    @property
    def done(self):
        rec = []
        for s in self.scans:
            rec.append({**s.key, 'done':s.done})
        return pd.DataFrame.from_records(rec)


# %%

if __name__ == "__main__":
    from qc import batch
    from qc import virtual as V

    scan_query = [
        (
            'study_name like "plat2oracle" '
            'and scan_purpose="platinum_plus" and '
            'notes not like "%%monitor issue%%" and '
            'notes not like "%%high monitor luminance%%"'
        ),
        (
            'study_name like "plat2dot2" '
            'and scan_purpose="platinum_plus"'
        ),
        (
            'study_name like "plat2oracler10" '
            'and scan_purpose="platinum_plus"'
        ),
        (
            'study_name like "plat2oracle_r10" '
            'and scan_purpose="platinum_plus" and score > 3'
        )
    ]

    scan_keys = (V.collection.CuratedScan & scan_query).fetch(
        "animal_id", "session", "scan_idx", as_dict=True
    )
    test_batch = batch.Batch(scan_keys)
# %%

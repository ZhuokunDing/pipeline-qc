import datajoint as dj
from . import virtual as V, scan, batch
schema = dj.schema('zhuokun_pipeline_qc', create_schema=True)

@schema
class PipelineQcJob(dj.Manual):
    definition = """
    -> V.experiment.Scan
    -> V.shared.PipelineVersion
    -> V.shared.SegmentationMethod
    -> V.shared.SpikeMethod
    -> V.shared.RegistrationMethod
    """

@schema
class PipelineQcDone(dj.Computed):
    definition = """
    -> V.experiment.Scan
    -> V.shared.PipelineVersion
    -> V.shared.SegmentationMethod
    -> V.shared.SpikeMethod
    -> V.shared.RegistrationMethod
    ---
    steps: varchar(1024)
    """

    @property
    def key_source(self):
        return PipelineQcJob

    def make(self, key):
        scan_qc = scan.Scan(**key)
        scan_qc.run_qc(
            filepath="/mnt/lab/users/zhuokun/pipeline_qc",
            steps='pupil-treadmill-rot',
            suppress_errors=False
        )
        self.insert1({**key, 'steps': 'pupil-treadmill-rot'})

    @property
    def current_batch(self):
        keys = (PipelineQcJob - self).fetch('KEY')
        return batch.Batch(keys)

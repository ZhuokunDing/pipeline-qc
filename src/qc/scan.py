from . import (
    virtual as V,
    pupil,
    treadmill,
    utils,
    jobs,
    stack,
)
from dataclasses import dataclass, asdict
from pathlib import Path
import pandas as pd


@dataclass
class Scan(dict):
    animal_id: int
    session: int
    scan_idx: int
    pipe_version: int = 1
    segmentation_method: int = 6
    spike_method: int = 6
    registration_method: int = 5


    @property
    def key(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}

    ## Useful queries
    @property
    def pipe(self):
        return utils.get_pipe(self.key)

    @property
    def done(self) -> bool:
        return bool(len(self.pipe.ScanDone & self.key))

    @property
    def stack(self):
        return stack.find_stack(self.key)
    
    @property
    def stack_reg(self):
        # check if CorrectionChannel is inserted for every scan field
        assert (
            len(self.pipe.CorrectionChannel & self.key) == self.nfields
        ), f"CorrectionChannel is not inserted for scan: {self.key}"
        stack_key = self.stack
        # check if scan is corrected
        assert (
            len(self.pipe.SummaryImages & self.key) == self.nfields
        ), f"SummaryImages missing for {self.key}"
        # check if CorrectionChannel is inserted for every stack channel, this is automatically done when inserting StackInfo if only one channle is recorded
        assert (
            len(V.stack.CorrectionChannel & stack_key) == 1
        ), f"CorrectionChannel is not inserted for stack: {stack_key}"
        # check if stack is corrected
        assert (
            len(V.stack.CorrectedStack & stack_key) > 0
        ), f"CorrectedStack missing for {stack_key}"
        # compose the query
        corrected_stack = (V.stack.CorrectedStack * V.stack.CorrectionChannel & stack_key).proj(
            stack_session="session", stack_channel="channel"
        )
        scan_fields = (self.pipe.ScanInfo * self.pipe.CorrectionChannel & self.key).proj(
            scan_session="session", scan_channel="channel"
        )
        reg_task = corrected_stack * scan_fields * V.shared.RegistrationMethod & self.key
        assert (
            len(reg_task) == self.nfields
        ), f"Number of fields do not match for {self.key}"
        return reg_task

    @property
    def fields(self):
        return self.pipe.ScanInfo.Field & self.key

    @property
    def nfields(self):
        return len(self.fields)

    @property
    def beh_h5_filepath(self):
        return utils.get_beh_h5_filepath(self.key)

    ## Monitoring pipeline progress
    @property
    def table_ls(self):
        return [
            self.pipe.ScanInfo,
            self.pipe.Quality,
            self.pipe.RasterCorrection,
            self.pipe.MotionCorrection,
            self.pipe.SummaryImages,
            self.pipe.Segmentation,
            self.pipe.Fluorescence,
            self.pipe.MaskClassification,
            self.pipe.ScanSet,
            self.pipe.Activity,
            self.pipe.ScanDone,
            V.stack.CorrectedStack,
            V.pupil.FittedPupil,
            V.treadmill.Treadmill,
            V.stack.PreprocessedStack,
        ]

    @property
    def auto_processing(self):
        return V.experiment.AutoProcessing & self.key

    @property
    def jobs_df(self):
        return jobs.get_jobs(jobs.jobs_schemas, [self.key,])
    
    @property
    def stack_jobs_df(self):
        return jobs.get_jobs({'stack': V.stack}, [self.stack, *self.stack_reg.fetch('KEY')])

    def delete_errors(self, errors=None):
        if errors is None:
            errors = (
                "LostConnectionError: Connection was lost during a transaction.",
                "OperationalError: (1205, 'Lock wait timeout exceeded; try restarting transaction')",
            )
        jobs.delete_errors(self.jobs_df, errors=errors)
        jobs.delete_errors(self.stack_jobs_df, errors=errors)

    @property
    def jobs_progress(self):
        for t in self.table_ls:
            print(t.full_table_name)
            print(t & self.key)

    def fill_registration_task(self, force=False):
        # print generated registration task and wait for user input
        reg_task = self.stack_reg
        print(reg_task)
        if (
            force
            or input("Confirm registration task for DataJoint insert. (y/n): ") == "y"
        ):
            # insert registration task
            V.stack.RegistrationTask.insert(
                reg_task, ignore_extra_fields=True, skip_duplicates=True
            )
            print("Registration task inserted.")
            return
        print("Registration task not inserted.")
        return
    
    def fill_rot_task(self, force=False):
        rot_task = self.stack_reg
        print(rot_task)
        if (
            force
            or input("Confirm RegistrationOverTime task for DataJoint insert. (y/n): ") == "y"
        ):
            # insert rotation task
            V.stack.RegistrationOverTimeTask.insert(
                rot_task, ignore_extra_fields=True, skip_duplicates=True
            )
            print("RegistrationOverTime task inserted.")
            return
        print("RegistrationOverTime task not inserted.")
        return

    ## Quality Control
    def treadmill_qc(self):
        return treadmill.treadmill_qc(self.key)

    def pupil_qc(self):
        return pupil.pupil_qc(self.key)

    # def segmentation_qc(self):
    #     return segmentation.segmentation_qc(self.key)

    # def motion_correction_qc(self):
    #     return motion_correction.motion_correction_qc(self.key)

    # def spike_qc(self):
    #     return spike.spike_qc(self.key)

    # def mask_classification_qc(self):
    #     return mask_classification.mask_classification_qc(self.key)
    def run_qc(self, filepath="/mnt/lab/users/zhuokun/pipeline_qc"):
        # create folder if not exist
        filepath = Path(filepath) / utils.dict2str(self.key)
        filepath.mkdir(parents=True, exist_ok=True)
        figs = []
        figs.append(self.pupil_qc())
        figs.append(self.treadmill_qc())
        # save figs to file as pdfs
        for name, fig in figs:
            fig.savefig(filepath / f"{name}.pdf")
        return figs


# %%
if __name__ == "__main__":
    from qc import virtual as V, scan

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
    scan_key = (V.collection.CuratedScan & scan_query).fetch(
        "animal_id", "session", "scan_idx", as_dict=True
    )[0]
    test_scan = scan.Scan(**scan_key)
# %%

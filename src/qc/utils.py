import datajoint as dj
import platform
import numpy as np
from pathlib import Path
from . import virtual as V


def get_pipe(scan_key):
    try:
        pipe = (dj.U("pipe") & (V.fuse.ScanSet & scan_key)).fetch1("pipe")
    except dj.DataJointError:
        if len(V.meso.ScanInfo & scan_key):
            pipe = "meso"
        elif len(V.reso.ScanInfo & scan_key):
            pipe = "reso"
        else:
            raise ValueError("Scan not found.")

    return getattr(V, pipe)


def get_linux_folder(key):
    assert "linux" in platform.system().lower()
    scan_path = (V.experiment.Session & key).fetch1("scan_path")
    path_df = V.lab.Paths().fetch(format="frame")
    matching = path_df.applymap(lambda x: x in scan_path)
    assert matching.to_numpy().sum() == 1
    matching = np.nonzero(matching.to_numpy())
    matching = np.array(matching).squeeze()
    scan_path = scan_path.replace("\\", "/").replace(
        path_df.to_numpy()[matching[0], matching[1]],
        path_df.iloc[matching[0]].linux,
    )
    return scan_path


def get_beh_h5_filepath(key):
    return Path(
        get_linux_folder(key)
        + f'/{key["animal_id"]}_{key["session"]}_{key["scan_idx"]:0>5}_beh.avi'
    )

def dict2str(dic):
    return '_'.join([f'{k.replace("_", "-")}-{v}' for k,v in dic.items()])
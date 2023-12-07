from . import MissingError, logger, virtual as V, utils
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt


def find_same_session_stack(scan: dict):
    stack = V.experiment.Stack & scan
    if len(stack) == 1:
        return stack.fetch1("KEY")
    elif len(stack) == 0:
        raise MissingError(f"Did not find any stack in the same session for {scan}.")
    else:
        raise ValueError(f"Found more than one stack for {scan}.")


def find_stack(scan: dict):
    stack_key = find_same_session_stack(scan)
    logger.info(f"Found same session stack for {scan}: {stack_key}")
    # TODO: add logic to find stack in nearest session if no stack in same session
    return stack_key


def rot_qc(rot_key_df: pd.DataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(10, 5))
    for rot_key in rot_key_df.sort_values("field").to_dict("records"):
        if rot_key["registration_method"] == 5:
            frame_num, reg_z = (V.stack.RegistrationOverTime.NonRigid & rot_key).fetch(
                "frame_num", "reg_z", order_by="frame_num"
            )
            # plot raw reg_z
            axes[0].plot(frame_num, reg_z, label=f"field {rot_key['field']}")
            axes[0].set_xlabel("frame_num")
            axes[0].set_ylabel("reg_z")
            axes[0].set_title("NonRigid registration")
            # plot median subtracted reg_z
            axes[1].plot(
                frame_num, reg_z - np.median(reg_z), label=f"field {rot_key['field']}"
            )
            axes[1].set_xlabel("frame_num")
            axes[1].set_ylabel("reg_z - median(reg_z)")
            axes[1].set_title("NonRigid registration")
        else:
            raise NotImplementedError("Only registration_method 5 is implemented")

    # plot +- 10 um lines for reference
    axes[1].plot([frame_num[0], frame_num[-1]], [-10, -10], "k--")
    axes[1].plot([frame_num[0], frame_num[-1]], [10, 10], "k--")
    # set legend outside of axes 1
    axes[1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    fig.tight_layout()
    rot_key = rot_key_df[
        [
            "animal_id",
            "stack_session",
            "stack_idx",
            "volume_id",
            "stack_channel",
            "scan_session",
            "scan_idx",
            "scan_channel",
            "registration_method",
        ]
    ].drop_duplicates()
    assert len(rot_key) == 1
    rot_key = rot_key.iloc[0].to_dict()
    return "rot_" + utils.dict2str(rot_key), fig

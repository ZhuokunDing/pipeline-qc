import pandas as pd
import numpy as np
import cv2
from matplotlib import pyplot as plt
from . import virtual as V, utils


def plot_pupil_fit(
    x,
    y,
    r,
    vid,
    points=None,
    crop=None,
    vmin=None,
    vmax=None,
    axes=None,
    sample_idx=None,
):

    if sample_idx is None:
        sample_idx = np.linspace(0, len(x) - 1, 50).astype(int)
    if axes is None:
        _, axes = plt.subplots(10, 10, figsize=[20, 20])
    for i, j in enumerate(sample_idx):
        vid.set(1, j)
        ret, pupil_frame = vid.read()
        pupil_frame = pupil_frame.mean(-1)
        if crop is not None:
            pupil_frame = pupil_frame[crop[2] : crop[3], crop[0] : crop[1]]
        col = i % 10
        track_row = i // 10 * 2
        fit_row = i // 10 * 2 + 1
        plt.sca(axes[track_row, col])
        _vmin = vmin or pupil_frame.min()
        _vmax = vmax or pupil_frame.max()
        plt.imshow(pupil_frame, vmin=_vmin, vmax=_vmax, cmap="gray")
        if points is not None:
            points_x = np.stack(points.x)[:, j]
            points_y = np.stack(points.y)[:, j]
            plt.scatter(points_x, points_y, color="r", s=1)
        plt.sca(axes[fit_row, col])
        plt.imshow(pupil_frame, vmin=_vmin, vmax=_vmax, cmap="gray")
        if x is not np.nan:
            circle = plt.Circle(
                (x[j], y[j]), r[j], fill=False, edgecolor="r", linestyle="--"
            )
            axes[fit_row, col].add_patch(circle)
    for ax in axes.ravel():
        ax.set_axis_off()


def pupil_qc(key, seed=0):
    rng = np.random.default_rng(seed)
    # fetch data
    crop = (V.pupil.Tracking.Deeplabcut & key).fetch1(
        "cropped_x0", "cropped_x1", "cropped_y0", "cropped_y1"
    )
    pupil_df = pd.DataFrame(
        (V.pupil.FittedPupil().Circle & key).fetch(
            "center", "radius", "KEY", as_dict=True, order_by="frame_id ASC"
        )
    )
    pupil_x = np.array(
        [coor[0] if coor is not None else np.nan for coor in pupil_df.center.to_numpy()]
    )
    pupil_y = np.array(
        [coor[1] if coor is not None else np.nan for coor in pupil_df.center.to_numpy()]
    )
    pupil_r = pupil_df.radius.to_numpy()
    nans = np.isnan(pupil_x) | np.isnan(pupil_y) | np.isnan(pupil_r)
    eye_points = pd.DataFrame(
        (V.pupil.FittedPupil.EyePoints() & key).fetch("x", "y", "label", as_dict=True)
    )
    assert len(eye_points) == 16

    # load eye video
    video_path = utils.get_beh_h5_filepath(key)
    assert video_path.is_file()
    pupil_video = cv2.VideoCapture(str(video_path))
    assert pupil_video.get(cv2.CAP_PROP_FRAME_COUNT) == len(pupil_x)

    # scan key title
    scan_key = f'{key["animal_id"]}-{key["session"]}-{key["scan_idx"]}'

    # figure layout
    fig = plt.figure(figsize=(20, 45))
    subfigs = fig.subfigures(3, 1, hspace=0.025, height_ratios=[1, 4, 4])

    # [Figure 1] check traces
    axes = subfigs[0].subplots(3, 1, sharex=True)
    plt.sca(axes[0])
    plt.plot(pupil_x, "k", linewidth=0.5)
    plt.ylabel("pupil center x")
    twinx = axes[0].twinx()
    twinx.plot(nans, "r", linewidth=0.5, alpha=0.2)
    plt.sca(axes[1])
    plt.plot(pupil_y, "k", linewidth=0.5)
    plt.ylabel("pupil center y")
    twinx = axes[1].twinx()
    twinx.plot(nans, "r", linewidth=0.5, alpha=0.2)
    plt.sca(axes[2])
    plt.plot(pupil_r, "k", linewidth=0.5)
    plt.ylabel("pupil radius")
    twinx = axes[2].twinx()
    twinx.plot(nans, "r", linewidth=0.5, alpha=0.2)
    subfigs[0].suptitle(
        f"{scan_key}\n{nans.sum()}/{len(pupil_r)} ({nans.sum()/len(pupil_r)*100:.2f}%) nans in total"
    )

    # [Figure 2] uniformly sample frames and check tracking and fitting
    axes = subfigs[1].subplots(10, 10)
    plot_pupil_fit(
        pupil_x, pupil_y, pupil_r, pupil_video, crop=crop, points=eye_points, axes=axes
    )
    subfigs[1].suptitle(scan_key + "uniformly sampled over time", y=0.9)

    # [Figure 3] check examples of tracking/fitting failures
    axes = subfigs[2].subplots(10, 10)
    nan_idx = np.nonzero(nans)[0]
    if len(nan_idx) > 50:
        nan_idx = sorted(rng.choice(nan_idx, 50, replace=False))
    plot_pupil_fit(
        pupil_x,
        pupil_y,
        pupil_r,
        pupil_video,
        crop=crop,
        points=eye_points,
        axes=axes,
        sample_idx=nan_idx,
    )
    subfigs[2].suptitle(scan_key + "nan examples", y=0.9)
    pupil_key = (V.pupil.FittedPupil & key).fetch1('KEY')
    return 'pupil_' + utils.dict2str(pupil_key), plt.gcf()

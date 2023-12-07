# %%
from matplotlib import pyplot as plt
from . import virtual as V, utils


# %%
def treadmill_qc(key):
    treadmill_key, treadmill_raw, treadmill_time, treadmill_vel = (
        V.treadmill.Treadmill() & key
    ).fetch1("KEY", "treadmill_raw", "treadmill_time", "treadmill_vel")
    fig, axes = plt.subplots(2, 1, figsize=[10, 5])
    axes[0].plot(treadmill_time, treadmill_raw, color="k")
    axes[0].set_ylabel("Treadmill Raw (cycles)")
    axes[1].plot(treadmill_time, treadmill_vel, color="k")
    axes[1].set_ylabel("Treadmill Velocity (cm/sec)")
    axes[1].set_xlabel("Time (s)")
    plt.suptitle(f'{key["animal_id"]}-{key["session"]}-{key["scan_idx"]}')
    plt.tight_layout()
    return "treadmill_" + utils.dict2str(treadmill_key), fig


# %%

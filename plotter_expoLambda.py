# import matplotlib
import os

import matplotlib.pyplot as plt
import numpy as np
# import numpy as np
import seaborn as sns
import pandas as pd

n = 100_000
NUM_RECORDS = 16
x = "Threads"
y = "Mixed Time"

lbs = ['0.1', '16', '32', '64', '128', '256', '512', '1024']

thresh_hold = [0.1, 0.3, 0.5, 0.7, 0.9]
reads_th = [0.9, 0.7, 0.5, 0.3, 0.1]
rq = [0.0, 0.1, 0.5, 0.9, 1.0]

rq_offset = [
    4 * (NUM_RECORDS + 1),
    64 * (NUM_RECORDS + 1),
]

threads_max = 64
ests = [np.average, ]  # np.min, np.max]

# dir_name = "1Mio"
dir_nameo = "DBS_16"
os.mkdir(dir_nameo)

# file_name = "1Mio"
file_name = "dbs_16"

df = pd.read_csv(file_name + ".csv", delimiter=",", decimal=".")

# df.drop('Run', axis=1, inplace=True)

# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("LockC") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("HL") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("MonoWriter") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("ORWC(Attempts=0") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("ORWC(Attempts=4") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("ORWC(Attempts=128") else v)
# df["Protocol"] = df["Protocol"].apply(
#          lambda v: "1" if str(v).startswith("ORWC(Attempts=64") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LockCoupling") else v)
# df["Protocol"] = df["Protocol"].apply(
#     lambda v: "1" if str(v).startswith("LHL(wAttempts=128") else v)
# df["Protocol"] = df["Protocol"].apply(
#     lambda v: "1" if str(v).startswith("LHL(wAttempts=64") else v)
# df["Protocol"] = df["Protocol"].apply(
#     lambda v: "1" if str(v).startswith("LHL(wAttempts=4") else v)
# df["Protocol"] = df["Protocol"].apply(
#     lambda v: "1" if str(v).startswith("LHL(wAttempts=0") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LHL(rAttempts=16") else v)
# # df["Protocol"] = df["Protocol"].apply(
# #         lambda v: "1" if str(v).startswith("LHL(wAttempts=16") else v)
# # df["Protocol"] = df["Protocol"].apply(
# #         lambda v: "1" if str(v).startswith("LHL(wAttempts=1)") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LHL(wAttempts=0;") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LHL(rAttempts=") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LHL(rAttempts=1)") else v)
# df["Protocol"] = df["Protocol"].apply(
#         lambda v: "1" if str(v).startswith("LHL(wAttempts=0") else v)

for lb in lbs:
    for est in ests:
        for th in thresh_hold:
            for rq_off in rq_offset:
                # dir_name = dir_nameo + "/rq_offset_" + str(rq_off / (NUM_RECORDS + 1))
                # try:
                #     os.mkdir(dir_name)
                # except:
                #     print()
                for rq_p in rq:
                    # try:
                    #     dirc = dir_name + "/rq_probability_" + str(rq_p)
                    #     os.mkdir(dirc)
                    # except:
                    #     print()
                    p = sns.relplot(x=x,
                                    y=y,
                                    # style="Protocol",
                                    hue="Protocol",
                                    kind="line",  # scatter
                                    # data=df,
                                    data=df.query("`U-TH` == " + str(th) +
                                                  " and `Threads` <= " + str(threads_max) +
                                                  " and `Protocol` != '1'"
                                                  " and `Lambda` == " + str(lb) +
                                                  " and `Range Offset` == " + str(rq_off) +
                                                  " and `RQ-TH` == " + str(rq_p)
                                                  # "and `Read Records` == '" + str(read_n) + "'"
                                                  ),
                                    estimator=est,
                                    ci=None,
                                    # palette="tab20"
                                    palette="tab20"
                                    )  # palette="tab10"

                    # p.set(xlabel=None)
                    p.set(yscale='log')
                    # p.set(xlabel=x)
                    p.set(ylabel="Time (ms)")
                    p.set(title="Lb=" + str(lb) +
                                ", U/R/RQ= " + str(th) + "/" + str(reads_th[thresh_hold.index(th)]) + "/" + str(rq_p) +
                                ", Pages=" + str(rq_off / (NUM_RECORDS + 1)))
                    # plt.legend(title="Traversal Strategy")
                    p.tight_layout()

                    save = file_name

                    if save and len(p.data) > 0:
                        plt.savefig(dir_nameo + "/Lb=" + str(lb) +
                                    ", U_R_RQ= " + str(th) + "_" + str(reads_th[thresh_hold.index(th)]) + "_" + str(rq_p) +
                                    ", Pages=" + str(rq_off / (NUM_RECORDS + 1)) + ".pdf")

            # plt.show()

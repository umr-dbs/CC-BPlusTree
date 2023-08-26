# import matplotlib
import os

import matplotlib.pyplot as plt
import numpy as np
# import numpy as np
import seaborn as sns
import pandas as pd

n = 100_000
x = "Threads"
y = "Mixed Time"

lbs = ['0.1', '0.2', '0.4', '0.6', '0.8',
       '1', '2', '4', '6', '8',
       '16', '32', '64', '128', '256', '512',
       '1024']

ests = [np.average, ]  # np.min, np.max]

# dir_name = "1Mio"
dir_nameo = "Hits_All"
os.mkdir(dir_nameo)

# file_name = "1Mio"
file_name = "~/CLionProjects/CC-BPlusTree/target/release/leaf_hits_lambda_"

for lb in lbs:
    df = pd.read_csv(file_name + str(lb) + ".csv", delimiter=",", decimal=".")
    NUM_RECORDS = int(df["Leaf Size"][0])
    N = int(df["N"][0])

    plt.figure(figsize=(10, 6))
    p = sns.histplot(data=df, x='Low', bins=150, weights='Count', stat='count', common_norm=False)

    # Customize plot appearance
    plt.xlabel('Leaf Nodes Key Interval')
    plt.ylabel('Registered Hits')
    plt.title('Lambda = ' + str(lb) + ", Max Keys per Leaf = " + str(NUM_RECORDS) + ", N = " + str(N))
    plt.xticks(rotation=45)
    plt.tight_layout()
    # plt.show()
    # p.set(yscale='log')
    p.set(ylabel="Hits")
    plt.savefig(dir_nameo + "/Hits_Lambda=" + str(lb) + "_Leaf_Size=" + str(NUM_RECORDS) + "_N=" + str(N) + ".pdf")

import numpy as np
import pandas as pd
from pandas_profiling import ProfileReport

def main():
    df = pd.DataFrame(
        np.random.rand(100, 5),
        columns=['a', 'b', 'c', 'd', 'e']
    )
    profile = ProfileReport(df, title='Pandas Profiling Report', html={'style': {'full_width': True}})

if __name__ == '__main__':
    main()

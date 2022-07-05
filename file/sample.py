import numpy as np
import pandas as pd

import io
df = pd.read_excel('add.xlsx')

print(df.head(5))

value = df[(df['격자 X'] == 60) & (df['격자 Y']== 127)]['행정구역코드']

print(value)

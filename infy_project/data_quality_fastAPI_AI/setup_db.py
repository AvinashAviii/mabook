import sqlite3
import pandas as pd


def init_db():
    conn = sqlite3.connect('local_dq.db')
    data = {
        'PART_NO': ['P001', None, 'P003', 'P004'],
        'SUPP_CD': ['S1', 'S2', None, 'S4'],
        'ORIGINATING_FLG': ['Yes', 'No', 'Yes', 'Invalid'],
        'ORIGINATING_PCT': [100, 0, 50, 110],
        'ACTV_IND': ['Y', 'N', 'Y', 'X']
    }
    df = pd.DataFrame(data)
    df.to_sql('supplier_data', conn, if_exists='replace', index=False)
    conn.close()
    print("Local database 'local_dq.db' created.")


if __name__ == "__main__":
    init_db()

from .load import read_file,preprocess_dataframe
from .transform.mtr import transform_mtr_df
from .transform.payment import transform_payment_df
def process_files(mtr_file, payment_file):
    # load data frame and preprocess
    mtr_df = preprocess_dataframe(read_file(mtr_file))
    payment_df = preprocess_dataframe(read_file(payment_file))

    # transform pandas data frame
    mtr_df = transform_mtr_df(mtr_df)
    payment_df = transform_payment_df(payment_df)

    print(mtr_df.head(3))
    print(payment_df.head(3))
import pickle
import pandas as pd

def analyze(context=None, results=None):
    print(df.columns)
    print(to_df(df, "transactions"))


def get_pickle_files(strategyFilename, startdate, enddate):
    """
    :param strategyFilename: the_simulate_strategy
    :param startdate: 20180516
    :param enddate: 20180517
    :return: file generator
    """
    start, end = int(startdate), int(enddate)
    for date in range(start, end+1):
        file = "%s-%s.pickle" % (strategyFilename, date)
        yield file

def read_all_pickle(strategyFilename, startdate, enddate):
    res = pd.DataFrame()
    for file in get_pickle_files(strategyFilename, startdate, enddate):
        perf = pd.read_pickle(file)
        res = res.append(perf)
    return res

def to_df(data,name):
    res = pd.DataFrame()
    for index,val in eval("data.{}.iteritems()".format(name)):
        if len(val) != 0:
            df = pd.DataFrame(data=val)
            df['dt'] = str(index)
            res = res.append(df)

    return res.set_index('dt')

if __name__ == "__main__":
    # print(to_df(perf,"orders"))
    # print(to_df(perf,"positions"))
    df = read_all_pickle("ths_simulate_strategy", "20180516", "20180517")
    analyze(context=None, results=df)
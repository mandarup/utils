
import os
import shutil


def clean_dir(directory):
    """Remove and add empty dir.
    
    Args:
        directory (str) : abs path to directory
    """
    
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)
    
    
def make_tarfile(output_filename, source_dir):
    """Make tarfile.
    
    Args:
        output_filename (str) : desired output file name with extension(.tar.gz)
            and absolute path
        source_dir (str) : abs path to dir to be compressed
    """
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
        

def print_column_names(X):
    for i in X.columns.tolist():
        print( X.columns.tolist().index(i), i)


def print_unique_values(df,column_name):
    for e,i in enumerate(df[column_name].unique()):
        print(e,i)
    print(len(df[column_name].unique()))



def dump_yaml(dump_obj, path):
    """Save config to disk as yaml file (.yml) in project directory"""
    with open(path, 'w') as yaml_file:
        yaml_file.write(yaml.dump(dump_obj, default_flow_style=False))
        yaml_file.flush()


def load_yaml(load_file):
    """Load config file in yaml format to dictionary

    Parameters
    ----------
    load_file: str
        path to file in yaml format

    Returns
    -------
    dict
        a dicitonary of parameters
    """
    prm = None
    if load_file is not None:
        with open(load_file, "r") as f:
            prm = yaml.safe_load(f)
    return prm


def write_csv(itemlist, filename):
    with open(filename, 'wb') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(itemlist)




# Print iterations progress
def print_progress(iteration, total, prefix = '', suffix = '', decimals = 1, barLength = 100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        barLength   - Optional  : character length of bar (Int)
    """
    formatStr       = "{0:." + str(decimals) + "f}"
    percents        = formatStr.format(100 * (iteration / float(total)))
    filledLength    = int(round(barLength * iteration / float(total)))
    bar             = '█' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration == total:
        sys.stdout.write('\n')
        sys.stdout.flush()


def compute_time_delta(start_time, prefix=''):
    """ print or return time (str h-m-s) since start_time

    Parameters
    ----------
    start_time: float
        time

    prefix: str
        prefix to printing time
        output: prefix + time

    print_time: boolean
        if True, then print time to console, else return time as string

    Returns
    -------
    timestr: str
        if print_time is None then return
    """
    stop = timeit.default_timer()
    seconds = stop - start_time
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    timestr = "%d:%02d:%02d" % (h, m, s)
    return timestr


def print_time_delta(start_time, prefix=''):
    time_delta = compute_time_delta(start_time, prefix=prefix)
    print(prefix + time_delta)


def dump(obj, filename):
    """Save object to disk

    This function provides flexibility to use tool of choice for dumping data

    Parameters
    ----------
    obj: any python object

    path: str
        destination dir and file name
    """
    # save object to disk
    joblib.dump(obj, filename)


def load(filename):
    """Load object from disk

    This function provides flexibility to use tool of choice for loading data

    Parameters
    ----------
    obj: any python object

    path: str
        destination dir and file name
    """
    # save object to disk
    return joblib.load(filename)



def gzip_dir(dir_in, dir_out):
    for root, dirs, files in os.walk(dir_in):
        for fname in files:
            filename = os.path.join(root, fname)
            if not os.path.exists(dir_out):
                os.mkdir(dir_out)
            out_file = os.path.join(dir_out, fname + '.gz')
            print(out_file)
            gzip_file(filename, out_file)


def gunzip_dir(dir_in, dir_out):
    for root, dirs, files in os.walk(dir_in):
        for fname in files:
            filename = os.path.join(root, fname)
            if not os.path.exists(dir_out):
                os.mkdir(dir_out)
            out_file = os.path.join(dir_out, fname.replace('.gz', ''))
            print(out_file)
            gunzip_file(filename, out_file)


def gzip_file(f_in, f_out):
    """Gzip file (.gz)""""
    with open(f_in, 'rb') as f_in, gzip.open(f_out, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def gunzip_file(f_in, f_out):
    """Unzip file (.gz)."""
    with gzip.open(f_in, 'rb') as f_in, open(f_out, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def pretty_print_title(title):
    """Print formatted str.
    
     prints string with following format
     .. 
               -----
               title
               -----
    """
    title_len = len(title)
    print('-' * title_len)
    print(title)
    print('-' * title_len)


def make_title(title):
    """Return formatted string.
    
    Returns:
      (str) : string with following format
          ..
               -----
               title
               -----
    """
    title_len = len(title)
    titlestr = '\n'
    titlestr += '-' * title_len
    titlestr += '\n' + title + '\n'
    titlestr += '-' * title_len
    return titlestr



def align_columns(X, training_features):
    """
    Check for differences in columns between saved dataset and trained model, so that we can reproduce the
    preprocessing and appy the model to a dataset with less columns (aggregated features) For example if we have a
    previously trained model with all companies, and we want to predict for just one company, the saved model has
    to be applied to a dataset that may contain fewer columns, and rarely some entirely new columns. We will
    address the first issue by appending missing columns and filling them with zeros, and the second issue by
    dropping the extra columns.
    _s stands for saved and _c for current


    Parameters
    ----------
    X: pandas dataframe of shape (n_test_samples, n_test_features)

    training_features: list
        features used for training
    """

    columns_s = training_features
    columns_c = list(X.columns.values)
    # print('num columns', len(columns_s), len(columns_c))

    # Drop columns on the processed dataset that are not part of the saved model
    diff_c_s = list(set(columns_c) - set(columns_s))
    if len(diff_c_s) > 0:
        X.drop(diff_c_s, axis=1, inplace=True)
        print('dropping columns')
        print(diff_c_s)

    # Append and fill with zeros columns of the saved model that do not exist in the processed dataset
    df_s = pd.DataFrame(columns=columns_s)
    X = df_s.append(X)
    X.fillna(0, inplace=True)

    # Reorder the columns according to saved model
    X = X[columns_s]

    return X


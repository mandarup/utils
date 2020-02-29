
import os
import glob


def format_file(filename);
    filename =  filename.replace(' ', '_').replace(',', '_')
    return filename

def rename():
    files = glob.glob('*.pdf')
    for f in files:
        f_out = format_file(f)
        os.rename(f, f_out)

if __name__ == '__main__':
    rename()

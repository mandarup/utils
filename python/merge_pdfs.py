"""Merge pdf files.

Dependencies:
pip install pypdf2
"""


import glob
from PyPDF2 import PdfFileWriter, PdfFileReader
import os
import datetime
from dateutil import parser
import sys

def merger(output_path, input_paths):
    pdf_writer = PdfFileWriter()

    if os.path.exists(output_path):
        print(f'Deleting {output_path}')
        os.remove(output_path)

    for path in input_paths:
        print(f'...Adding {path}')

        pdf_reader = PdfFileReader(path)
        for page in range(pdf_reader.getNumPages()):
            pdf_writer.addPage(pdf_reader.getPage(page))

    with open(output_path, 'wb') as fh:
        pdf_writer.write(fh)


if __name__ == '__main__':

    if len(sys.argv) < 3:
        raise Exception("Provide two positional args: input dir  and output file name")

    src = sys.argv[1]
    dst = sys.argv[2]
    print(src, dst)

    paths = glob.glob(src + '/*.pdf')
    paths.sort(reverse=True)

    print(f'all files: {len(paths)}')
    print(paths)

    merger(dst, paths)

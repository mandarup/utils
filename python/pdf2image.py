"""Convert pdf to image.

Requirements:
    pdf2image

References:
    - https://pypi.org/project/pdf2image/


"""

import pdf2image

from pdf2image.exceptions import (
    PDFInfoNotInstalledError,
    PDFPageCountError,
    PDFSyntaxError
)

import sys


def convert(src, dst):
    images_from_path = pdf2image.convert_from_path(src, output_folder=dst)
    return None



if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise Exception("Provide two positional args: input dir  and output file name")

    src = sys.argv[1]
    dst = sys.argv[2]
    print(src, dst)

    convert(src, dst)

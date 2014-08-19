import sys
import math

"""
Computes the number of parts that a bunch of several files should be split in order to get N homogeneous parts.
"""


# Hadoop's deflate ratio is slightly bigger than Java compression files.
DEFLATE_GZIP_HADOOP_RATIO = 1.365

TOLERANCE_RATIO = 0.1562

def printUsage():
    print "Usage: python number_splits.py <number of files> <total size in bytes> <block size>"

def should_split(number_of_files, total_size_bytes, block_size_bytes):

    if number_of_files == 0:
        return False
    elif (number_of_files == 1) and (total_size_bytes <= block_size_bytes):
        return False
    elif (number_of_files == 1) and ( total_size_bytes < block_size_bytes + (block_size_bytes * TOLERANCE_RATIO) ):
        return False
    else:
        return True


if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[1] == "" or sys.argv[2] == "" or sys.argv[3] == "":
        printUsage()
    else:
        number_of_files = int(sys.argv[1])
        total_size_bytes = int(sys.argv[2])
        block_size_bytes = int(sys.argv[3])

        total_size_deflate = DEFLATE_GZIP_HADOOP_RATIO * total_size_bytes

        if should_split(number_of_files, total_size_deflate, block_size_bytes):
            remaining_bytes = total_size_deflate % block_size_bytes

            if remaining_bytes <= (TOLERANCE_RATIO * block_size_bytes):
                round_down = max(1, int(round(total_size_deflate / block_size_bytes)))
                print '%s' % ( str(round_down))
            else:
                ceil = max(1, int(math.ceil(total_size_deflate / block_size_bytes)))
                print '%s' % ( str(ceil))
        else:
            print '0'

        
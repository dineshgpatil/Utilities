
'''

    :Created By: Dineshsing Patil
    :Created on: 10/10/2021
    :Purpose: Compare two csv files based on a single common key/column.

    :Last Modified on: 10/10/2021
    :Last Modified to:

'''

from diffUtility.common.diffUtil import diffUtil as dfUtil

if __name__ == '__main__':

    # Set this to the column name that's common to both files. Currently this supports a single column.

    lstKeyCols = ['Key']

    dfUtil.compare2FilesWithOneKey('./samples/sampleFile1.csv','./samples/sampleFile2.csv',lstKeyCols,'./report/diff_report.csv')




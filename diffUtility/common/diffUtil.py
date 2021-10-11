
'''

    :Created By: Dineshsing Patil
    :Created on: 10/8/2021
    :Purpose: compare two csv files based on a single common key/column.

    :Last Modified on:
    :Last Modified to:

'''

import pandas as pd

class diffUtil:

    def __init__(self):
        pass

    def getColumnDifferences(self,df1,df2):
        pass


    def getMissingColumns(self,df1,df2):

        #   Set count to 0 for missing columns

        pass

    def getMissingRows(self,):
        pass

    def printDuplicateItems(self,lstItems,fileName):
        if len(lstItems) > 0:
            raise ValueError('Script execution aborted as duplicate column/s {} exist in file {}'.format(
                lstItems,fileName))

    def diffSetsWithSingleKey(self):
        pass

    def diffSetsWithCompositeKey(self):
        pass

    def convertDFColsFromFloat64ToInt64(df):
        cols = df.columns.tolist()
        for col in cols:
            df[col] = df[col].astype('Int64')
        return df

    def getDictOfNaNsFromSet(inFile):

        '''

        :Added By: Dineshsing Patil
        :Added on to: 10/8/2021 : get a dictionary of all null values from a given file
        :Last Modified on to:
        :Parameters: a .csv file
        :return: original contents of the file, dictionary of Null values, dictionary of non-Null values
        '''

        dfTemp = pd.read_csv(inFile)
        dictNansFromSet1 = dfTemp.isnull().sum(axis=0).to_dict()
        dictNotNansFromSet1 = dfTemp.notnull().sum(axis=0).to_dict()
        return (dfTemp,dictNansFromSet1,dictNotNansFromSet1)

    def compare2FilesWithOneKey(file1,file2,lstKeyCols,diffReportOutputFile):

        '''

                :Added By: Dineshsing Patil
                :Added on to: 10/8/2021 : compare two csv files based on a single common key/column. The file itself
                                          could have multiple keys however this function is designed to compare all
                                          records using a single key.
                :Last Modified on to:
                :Parameters: two .csv files
                :return: original contents of the file, dictionary of Null values, dictionary of non-Null values
        '''

        df1, dictNansFromSet1, dictNotNansFromSet1 = diffUtil.getDictOfNaNsFromSet(file1)
        df2, dictNansFromSet2, dictNotNansFromSet2 = diffUtil.getDictOfNaNsFromSet(file2)

        df1Cols = sorted(df1.columns.tolist())
        df2Cols = sorted(df2.columns.tolist())

        commonColumns, uncommonColumns = diffUtil.getCommonAndUncommonFrom2Sets(df1Cols, df2Cols)

        for element in lstKeyCols:
            commonColumns.remove(element)

        for item in lstKeyCols:
            df1Cols.remove(item)
            df2Cols.remove(item)

        dfMerged = pd.merge(df1, df2, how='outer', on=lstKeyCols)
        dfCommonRows = pd.merge(df1, df2, how='inner', on=lstKeyCols)

        dictMatches, dictMisMatches, dictTotal = diffUtil.getDiffsForCommonColumns(commonColumns, uncommonColumns,
                                                                                 dfMerged, dfCommonRows)

        dfFinal = diffUtil.getDiffCounts(dictMatches, dictMisMatches, dictTotal, dictNansFromSet1,
                                           dictNotNansFromSet1, dictNansFromSet2,
                                           dictNotNansFromSet2)

        dfFinal.to_csv(diffReportOutputFile)

    def getNaNsPerDataFrameColumn(df):
        dictNans = df.isnull().sum(axis=0).to_dict()
        return dictNans

    def getCommonAndUncommonFrom2Sets(set1,set2):
        commonColumns = set(set1) & set(set2)
        uncommonColumns = set(set1).symmetric_difference(set(set2))
        return commonColumns,uncommonColumns

    def getDiffsForCommonColumns(commonColumns,uncommonColumns,dfMerged,dfCommonRows):

        dictMatches = {}
        dictMisMatches = {}
        dictTotal = {}

        for col in commonColumns:
            dfSubFrame = dfMerged[[col + '_x','Key',col + '_y']]
            dfCommonVals = dfSubFrame[dfSubFrame[col + '_x'] == dfSubFrame[col + '_y']]
            dictMatches[col] = dfCommonVals.shape[0]
            dictMisMatches[col] = dfSubFrame.shape[0] - dictMatches[col]
            dictTotal[col] = dictMatches[col] + dictMisMatches[col]
        dictMatches['Key'] = dfCommonRows.shape[0]
        dictTotal['Key'] = dictMatches['Key']
        dictMisMatches['Key'] = dfMerged.shape[0] - dfCommonRows.shape[0]
        dictTotal['Key'] = dictMisMatches['Key']

        for c in uncommonColumns:
            dictMisMatches[c] = dfMerged.shape[0]
            dictTotal[c] = dictMisMatches[c]

        return dictMatches,dictMisMatches,dictTotal


    def getDiffCounts(dictMatches,dictMisMatches,dictTotal,dictNansFromSet1,dictNotNansFromSet1,dictNansFromSet2,dictNotNansFromSet2):

        dfFinal = pd.DataFrame()
        dfFinal = dfFinal.append(dictMatches, ignore_index=True)
        dfFinal = dfFinal.append(dictMisMatches, ignore_index=True)
        dfFinal = dfFinal.append(dictTotal, ignore_index=True)
        dfFinal = dfFinal.append(dictNansFromSet1, ignore_index=True)
        dfFinal = dfFinal.append(dictNotNansFromSet1, ignore_index=True)
        dfFinal = dfFinal.append(dictNansFromSet2, ignore_index=True)
        dfFinal = dfFinal.append(dictNotNansFromSet2, ignore_index=True)
        dfFinal = dfFinal.T

        dfFinal.rename(columns={0: 'Common', 1: 'UnCommon', 2: 'Total', 3: 'NULLsFromSet1', 4: 'NotNULLsFromSet1',
                                5: 'NULLsFromSet2', 6: 'NotNULLsFromSet2'}, inplace=True)
        dfFinal = diffUtil.convertDFColsFromFloat64ToInt64(dfFinal)

        return dfFinal
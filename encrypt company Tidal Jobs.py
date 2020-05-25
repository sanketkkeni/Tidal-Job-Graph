# -*- coding: utf-8 -*-
"""
Created on Mon May 11 07:23:36 2020

@author: Sanket Keni
@Info: Created to hide the real JobNames/PackageNames from the project that I worked on
"""
import pandas as pd
from collections import OrderedDict
xl = pd.ExcelFile("C:\\Users\\sanke\\Desktop\\chs\\_src\\Metadata.SSIS\\Tidal Master Key.xlsx")
df = xl.parse("Sheet2")
df.head
df['Key']=df.Key.astype(str)

#Create ordered Job dict
df_temp = df[['Job Path','Key']]
df_temp['length'] = df_temp['Job Path'].str.len()
df_temp.sort_values('length', ascending=False, inplace=True)
orderedJobDict = OrderedDict() 
for index, row in df_temp.iterrows():
    #print(row['length'])
    orderedJobDict[row['Job Path']] = row['Key']

##Create ordered package dict
df_packages = df[df['SSIS Packages'].notnull()]['SSIS Packages']
packages_raw_list = df_packages.tolist()
package_actual_list = []
for i in packages_raw_list:
    for j in i.split(","):
        j = j.strip()
        package_actual_list.append(j)
        
package_actual_list = list(set(package_actual_list)) #Remove duplicates
package_actual_list_ordered = sorted(package_actual_list, key=len, reverse = True)
package_dict_ordered = OrderedDict() 
for i in range(len(package_actual_list_ordered)-1):
    package_dict_ordered[package_actual_list_ordered[i]] = str(i)
##--Create package dict

def convertActualNamesToPseudnym(actualName,dictionary, ReplacedValueBegin, ReplacedValueEnding):
    if actualName == ' ':
        return ' '
    pseudoName = actualName
    for key, value in dictionary.items():
        if key in actualName:
            pseudoName = pseudoName.replace(key, ReplacedValueBegin + value + ReplacedValueEnding)
    return pseudoName

df['Dependency List'].fillna(' ', inplace = True) 
df['Dependency List PseudoName'] = df.apply(lambda row : convertActualNamesToPseudnym(row['Dependency List'],orderedJobDict, 'Tidal_Job_', ''), axis = 1)

df['Successor List'].fillna(' ', inplace = True) 
df['Successor List PseudoName'] = df.apply(lambda row : convertActualNamesToPseudnym(row['Successor List'],orderedJobDict, 'Tidal_Job_', ''), axis = 1)

df['Job Path'].fillna(' ', inplace = True) 
df['Job Path PseudoName'] = df.apply(lambda row : convertActualNamesToPseudnym(row['Job Path'],orderedJobDict, 'Tidal_Job_', ''), axis = 1)


df['SSIS Packages'].fillna(' ', inplace = True) 
df['SSIS Packages PseudoName'] = df.apply(lambda row : convertActualNamesToPseudnym1(row['SSIS Packages'],package_dict_ordered, 'SSIS_Package_', '.dtsx'), axis = 1)

df = df[['Key', 'Job Path PseudoName', 'SSIS Packages PseudoName', 'Subgraph Number', 'Jobs in this Subgraph', 'Job Enabled?', 'color', 'project', 'Dependency AND/OR', 'Dependency Count', 'Dependency List PseudoName', 'Successor Count', 'Successor List PseudoName']]
df.columns = ['Key', 'Job Path', 'SSIS Packages', 'Subgraph Number', 'Jobs in this Subgraph', 'Job Enabled?', 'color', 'project', 'Dependency AND/OR', 'Dependency Count', 'Dependency List', 'Successor Count', 'Successor List']

df.to_csv('C:\\out\\Tidal_Master_Key_PseudoNames.csv')

###########################
# **********************************************************************************************************************
#     Program Name            -  POSITION_CALCULATION_PROCESS
#     Created By              -  PAPIL PATIL (papil.patil15@gmail.com)
#     Created Date            -  20-SEP-2018
#     Modified By             -  PAPIL PATIL (papil.patil15@gmail.com)
#     Modification Date       -  20-SEP-2018
#     Changes Made            -  Published the code on GitHub
#     Version No              -  1.0
#     Source File Format      -  CSV, JSON                                                             
#     Target File Format      -  CSV                                                                                    
#     Purpose                 -  At the end of the process find instruments with largest and 
#                                lowest net transaction volumes for the day.                                
#     Expected Output         -  Expected_EndOfDay_Positions.txt 
#                                (Output of the process should match with already provided Expected output)
#     Frequency               -  NA                                                                                                       
#     Temporary Table         -  NA
# **********************************************************************************************************************/


# ***** Import Libraries *****

import pandas as pd
import numpy as np
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# ***** Variable Declarations for Source Files path inputs *****

varStartPos = 'C:/Users/papil.patil/My_Notebooks/for_ubs/Input_StartOfDay_Positions.txt'
varInputTrx = 'C:/Users/papil.patil/My_Notebooks/for_ubs/Input_Transactions.txt'
varExpEodOutput = 'C:/Users/papil.patil/My_Notebooks/for_ubs/Expected_EndOfDay_Positions_by_Papil.txt'


# ***** Load Input_StartOfDay_Positions.txt CSV File to be used as an Input *****

try:
    dfStartPos = pd.read_csv(varStartPos)
except NameError:
        print "File Not Found.", 
except Exception, e:
        print "Error in reading",varStartPos
        print e


# ***** Load Input_Transactions.txt JSON File to be used as another Input *****

try:
    dfTrx = pd.read_json(varInputTrx)
except NameError:
        print "File Not Found.", 
except Exception, e:
        print "Error in reading",varInputTrx
        print e

dfTrx = dfTrx[['Instrument','TransactionType','TransactionQuantity']]


# ***** Aggregate Transaction Qty based on Instruments and Transaction Type (B/S) *****

dfTrxAgg = dfTrx.groupby(['Instrument','TransactionType'])['TransactionQuantity'].sum().reset_index()


# ***** Join Positions and Transactions data to get Instrument and Account Type wise TransactionType and Quantity *****

dfTrxAll = pd.merge(dfStartPos,dfTrxAgg,how='left', on='Instrument')


# ***** Rank all the Transactions according to Instrument and Account Type to get Start and End of Transactions 
# ***** for doing Cummulative Debit Credit Calculation

dfTrxAll['rnk'] = dfTrxAll.sort_values(['Quantity','TransactionType'], ascending=[True,True]) \
             .groupby(['Instrument','AccountType']) \
             .cumcount() + 1

        
# ***** If there is no Transaction found for a Intrument, fill Transaction Qty as Zero *****

dfTrxAll = dfTrxAll.fillna(0)


# ***** Apply Process Logic based on TransactionType and AccountType to revise Quantity *****

dfTrxProcess = dfTrxAll

def applyLogic(row):
    if row['TransactionType'] == 'B':
        if row['AccountType'] == 'I':
            return (row['TransactionQuantity'] * -1)
        elif row['AccountType'] == 'E':
            return (row['TransactionQuantity'] * 1)

    elif row['TransactionType'] == 'S':
        if row['AccountType'] == 'E':
            return (row['TransactionQuantity'] * -1)
        elif row['AccountType'] == 'I':
            return (row['TransactionQuantity'] * 1)
        
    else:
        return row['TransactionQuantity']

dfTrxProcess['TransactionQuantity'] = dfTrxProcess.apply(applyLogic, axis=1) 


# ***** Create Final Dataset with calculated Positions at End Of Day *****

dfTrxProcessFinal = dfTrxProcess

def newQty(row):
    if row['rnk'] == 1:
        return (row['TransactionQuantity']+row['Quantity'])
    else:
        return row['TransactionQuantity']

dfTrxProcessFinal['NewQuantity'] = dfTrxProcessFinal.apply(newQty, axis=1) 

dfTrxProcessFinal['NewQuantity'] = dfTrxProcessFinal['NewQuantity'].astype(int)

dfTrxProcessFinal = dfTrxProcessFinal.groupby(['Instrument','Account','AccountType'])['NewQuantity'].sum().reset_index()


# ***** Derivation for Delta (Net Change in Volume) based on Start Positions and End Positions*****

dfTrxProcessExpOutput = pd.merge(dfStartPos, dfTrxProcessFinal, how='left', on=['Instrument','Account','AccountType'])

dfTrxProcessExpOutput['Delta'] = dfTrxProcessExpOutput['NewQuantity']-dfTrxProcessExpOutput['Quantity'].astype(int)

dfTrxProcessExpOutput = dfTrxProcessExpOutput[['Instrument','Account','AccountType','NewQuantity','Delta']]

dfTrxProcessExpOutput.rename(columns={'NewQuantity': 'Quantity'}, inplace=True)


# ***** Save Expected EoD Output to a CSV file *****

dfTrxProcessExpOutput.to_csv(varExpEodOutput, index=False )


# ***** Print Expect EoD Output in Console *****

print("\n*************************************************************************************")
dfTrxProcessExpOutput.head(100)
print("\n*************************************************************************************\n")


# ***** Print instruments with largest and lowest net transaction volumes for the day in Console *****

print("\nInstrument with Larget Net Transaction Volumne for the days is : \n")
dfLargest = dfTrxProcessExpOutput[dfTrxProcessExpOutput['Delta'] == dfTrxProcessExpOutput['Delta'].max()]
print (dfLargest.to_string(index=False))

print("\n*************************************************************************************\n")

print("\nInstrument with Lowest Net Transaction Volumne for the days is : \n")
dfLargest = dfTrxProcessExpOutput[dfTrxProcessExpOutput['Delta'] == dfTrxProcessExpOutput['Delta'].min()]
print dfLargest.to_string(index=False)


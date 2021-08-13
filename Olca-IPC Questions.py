# -*- coding: utf-8 -*-
"""
Created on Fri Aug 13 16:11:13 2021

@author: sjoerd3125
"""

import pandas as pd
import olca
import uuid
import math
from datetime import datetime

from matplotlib import pyplot as plt
import matplotlib.mlab as mlab
from matplotlib import rcParams
import matplotlib.patches as mpatches
import seaborn as sns

import olca
from olca import ipc as ipc
from datetime import datetime
import pytz

# Honestly don't know what this is
params = {'mathtext.default': 'regular' }
# connect to OpenLCA
client = olca.Client(8080)



#%% setting up the calculation method

def Calculation_Setup(client, new_system):
    """
    

    Parameters
    ----------
    client : ipc.Client
        The OpenLCA client connector
    
    new_system : schema.ProductSystem
        New product system built.

    Returns
    -------
    result : schema.SimpleResult
        The calculation setup

    """
    setup = olca.CalculationSetup()
    setup.calculation_type = olca.CalculationType.UPSTREAM_ANALYSIS
    setup.product_system = olca.ref(
        olca.ProductSystem,
        new_system.id)
    setup.impact_method = olca.ref(
        olca.ImpactMethod,
        '31abcf53-5872-32ce-8ffc-1082f3fa3ed5') # This is CML 2001
        #'84b7e3f4-2898-3d5a-980a-faea0b995bdb') # This is the ILCD 2011  UUID
    
    setup.amount = 1.0
    
    result = client.calculate(setup)
    
    client.excel_export(result, 'String to export')
    
    client.dispose(result)
    #print(response)
    return

#%% Getting the information out of the excel sheet

def Get_Calc_Info(system_id, process_id):
    """
    Reads the important excel information and makes a list to append
    to an array to make a dataframe
    
    Parameters
    ----------
    system_id : str
        The id of the product system
    process_id : str
        The id of the process system
    Returns
    -------
    Process_Info : List
        List of Country, Reference flow, and all impacts of ILCD 2018.

    """
    
    # Read the Excel that was created
    Results = pd.read_excel('String to export',
                            sheet_name = 'Impacts',
                            usecols=[2,3,4],
                            header=1)
    Process_Info = pd.read_excel('String to export',
                            sheet_name = 'Calculation setup', 
                            usecols=[1,2],
                            skiprows=[0], index_col=0)
    Process_Info = [Process_Info.loc['Reference process:'].values[0],
                    Process_Info.loc['Reference process location:'].values[0],
                    Process_Info.loc['Amount:'].values[0],
                    system_id,
                    process_id] + list(Results.Result)
    return Process_Info

#%% Calculating Product Systems
all_processes = client.get_descriptors(olca.Process)
#Look for processes that contain some text you want to match
target_processes = [
    x
    for x in all_processes
    if "salmon, at fish farm" in x.name
]

datetime_str = datetime.now(pytz.utc).isoformat()


Process_Array = []
Error_Array = []

# https://ask.openlca.org/3123/how-create-productsystem-calculation-with-olca-olca-package
for index,olca_proc in enumerate(target_processes):
    print('At process', index + 1, 'of', len(target_processes))
    try:
        new_system_ref = client.create_product_system(
            process_id=olca_proc.id,
            default_providers="only",
            preferred_type="SYSTEM_PROCESS",
        ) #This returns a reference, not an olca.ProductSystem
        new_system=client.get(olca.ProductSystem,new_system_ref.id)
        new_system.olca_type = olca.schema.ProductSystem.__name__
     
        new_system.description = ("Test for OLCA-IPC integration in project 05-067-21")
        #name the product system the same as your process
        new_system.name = olca_proc.name
        model_type_str = f"{olca.ModelType.PRODUCT_SYSTEM}"
        new_system.version = "1.0.0"
        new_system.olca_type = olca.schema.ProductSystem.__name__
        new_system.category_path = []
        new_system.last_change = datetime_str
        # Do the calculation
        Calculation_Setup(client, new_system)
        # Get the info from the excel sheet
        Process_Info = Get_Calc_Info(new_system.id, olca_proc.id)
        # Add to array
        Process_Array.append(Process_Info)
        
        # Empty the system as noted in the documentation
        client.update(new_system)
        
        #if index == 10:
            #break
    except:
        print("System could not calculate, saved information.")
        Error_Array.append([index, new_system.id, olca_proc.id])

Process_Dump = pd.read_csv('String to export')

Process_Columns = Process_Dump.columns

Process_Dump = pd.DataFrame(data=Process_Array, columns=Process_Columns)

Error_Dump = pd.DataFrame(data=Error_Array, columns = ['index', 'system_id', 'process_id'])

Process_Dump.to_csv('String to export')

Error_Dump.to_csv('String to export')

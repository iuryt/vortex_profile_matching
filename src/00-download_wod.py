import warnings
import s3fs
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import os
from datetime import datetime
from glob import glob
import logging
import gc

# import dask
# from dask.distributed import Client, LocalCluster
# cluster = LocalCluster(n_workers=24)
# client = Client(cluster)

from dask.diagnostics import ResourceProfiler
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask

log_dir = "../tmp"
cluster = SLURMCluster(
    cores=1,
    memory="30GB",
    walltime='15:00:00',
    job_extra_directives=[
        f"--output={log_dir}/slurm-%j.out",
        f"--error={log_dir}/slurm-%j.err"
    ],
    scheduler_options={"dashboard_address": ":4242"}
)
cluster.scale(jobs=40)
client = Client(cluster)


variables = ["Temperature", "Salinity", "Oxygen", "Chlorophyll", "pH", "Nitrate"]

datasets = ["pfl", "ctd", "xbt", "apb", "gld"]

min_depth = 300

quality_control = [0, 2]
min_local_depth = 100
max_depth = 2000

overwrite = True

for dataset in datasets:
    path = f"../data/processed/wod/{dataset}"
    os.makedirs(path, exist_ok=True)
    for variable in variables:
        os.makedirs(f"{path}/{variable}", exist_ok=True)

def download_wod(year,dataset,variable,output="../data/processed/wod/",min_depth=min_depth,quality_control=quality_control, min_local_depth=min_local_depth, max_depth=max_depth):

    wod, bat, subset, f2 = None, None, None, None
    local_depth, rowsizes, profile_depth = None, None, None
    try:
        fs = s3fs.S3FileSystem(anon=True)
        url = f's3://noaa-wod-pds/{year}/wod_{dataset}_{year}.nc'
        f2 = fs.open(url)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            wod = xr.open_dataset(f2, engine='h5netcdf').load()
        
        keep_casts = np.array([True] * wod.casts.size)
        
        nan_all_vars = []
        if variable in list(wod):
            nan_all_vars.append(np.isnan(wod[f"{variable}_row_size"].values))
        nan_all_vars = np.vstack(nan_all_vars)
        keep_casts[nan_all_vars.all(0)] = False
        
        bat = xr.open_dataset("../data/external/gebco/GEBCO_2022_sub_ice_topo.nc")
        bat = bat.elevation.isel(lon=slice(None, None, 10), lat=slice(None, None, 10)).load()
        local_depth = -bat.sel(lon=wod.lon, lat=wod.lat, method="nearest").values
        keep_casts[local_depth < min_local_depth] = False
        
        
        rowsizes = wod.z_row_size.values.astype("int")
        z = wod.z.values
        
        profile_depth = []
        i = 0
        for rowsize in rowsizes:
            profile_depth.append(z[i : i + rowsize].max())
            i = i + rowsize
        profile_depth = np.array(profile_depth)
        keep_casts[profile_depth < min_depth] = False
        
        
        nlevels = np.ceil(np.quantile(wod.z_row_size.values.astype("int"), 0.995)).astype("int")
        
        if keep_casts.sum() > 1:
            qc_bool = []
            for qc in quality_control:
                qc_bool.append((wod[f"{variable}_WODprofileflag"]==qc).values)
            qc_bool = np.vstack(qc_bool)
            # True for casts that has any of the chosen qc flags
            qc_bool = np.any(qc_bool,0)
            
            var_rowsize = wod[f"{variable}_row_size"].fillna(0).values.astype("int")
            z_rowsize = wod["z_row_size"].fillna(0).values.astype("int")
            
            keep_casts[var_rowsize==0] = False
            keep_casts[~qc_bool] = False
            
            levels = []
            keep_obs = []
            casts=[]
            for i in np.arange(var_rowsize.size):
                if (var_rowsize[i]!=0):
                    keep_obs.append([keep_casts[i]]*var_rowsize[i])
                    levels.append(np.arange(var_rowsize[i]))
                    casts.append([i]*var_rowsize[i])
            casts = np.hstack(casts)
            levels = np.hstack(levels)
            keep_obs = np.hstack(keep_obs)
            index = np.argwhere(keep_obs).ravel()

            if keep_obs.sum() > 1:
                subset = (
                    wod[[variable, f"{variable}_WODflag"]]
                    .rename({f"{variable}_obs":"obs"})
                    .assign_coords(casts=("obs",casts),levels=("obs",levels))
                    .isel({"obs":index})
                )
                
                keep_obs = []
                for i in np.arange(z_rowsize.size):
                    if (z_rowsize[i]!=0):
                        keep_obs.append([keep_casts[i]]*z_rowsize[i])
                keep_obs = np.hstack(keep_obs)
                index = np.argwhere(keep_obs).ravel()
                subset = subset.assign_coords(z=wod["z"].isel({"z_obs":index})).rename({"z_obs":"obs"})
                
                subset = subset.where(subset[f"{variable}_WODflag"]==0,drop=True)
                subset = subset.drop_vars(f"{variable}_WODflag")
                
                subset = subset.where(subset.z < max_depth, drop=True)
                
                ind = np.argwhere(subset.levels.values <= nlevels).ravel()
                subset = subset.isel(obs=ind)
                
                # reset_levels = lambda ds: ds.assign(levels=("obs", np.arange(ds.levels.size)))
                # subset = subset.groupby("casts").apply(reset_levels)
                
                subset = subset.set_index(obs=["casts", "levels"]).unstack("obs")
                
                extra_variables = [
                    "wod_unique_cast", "WOD_cruise_identifier",
                    "Platform", "Project", "Ocean_Vehicle",
                    "WMO_ID", "lon", "lat", "time"
                ]
                
                meta_variables = list(set(extra_variables).intersection(set(list(wod))))
                for meta in meta_variables:
                    subset[meta] = wod[meta].isel(casts=subset.casts.values)

                
                # rename to lowecase because I hate having to use shift
                subset = subset.rename({k:k.lower() for k in list(subset.keys())})
                
                subset = subset.set_index(casts="wod_unique_cast")
    
                encoding={key:dict(_FillValue=9e4) for key in list(subset.variables)}
                filename = f"{dataset}/{variable}/{dataset}_{variable}_{year}.nc"
                filepath = os.path.join(output,filename)
                subset.to_netcdf(filepath, encoding=encoding)
    
                del wod, bat, subset, f2, local_depth, rowsizes, profile_depth
                gc.collect()
    except Exception as e:
        del wod, bat, subset, f2, local_depth, rowsizes, profile_depth
        gc.collect()
        return (year, dataset, variable, False, str(e)) 

    return (year, dataset, variable, True, "none")


base = "../data/processed/wod"
obj = []
for year in np.arange(1993,2023+1)[::-1]: 
    for dataset in datasets: 
        for variable in variables:
            obj.append(dask.delayed(download_wod)(year,dataset,variable))

# Parallel Computation and Error Handling
future = dask.compute(*obj)
future_df = pd.DataFrame(
    future, columns=["year", "dataset", "variable", "success", "error_message"]
)
future_df.to_csv("../tmp/download_wod.csv")

for a in future_df[~future_df["success"]].values:
    print(a)




variables = ["Temperature", "Salinity", "Oxygen", "Chlorophyll", "pH", "Nitrate"]

datasets = ["pfl", "ctd", "xbt", "apb", "gld"]

for dataset in datasets:
    for variable in variables:
        path = f"../data/processed/wod/{dataset}/{variable}"  # Construct the path
        if os.path.exists(path):  # Check if the directory exists
            files = os.listdir(path)  # List files in the directory
            if not files:  # If no files exist in the directory
                os.rmdir(path)  # Remove the directory
                print(f"Deleted empty directory: {path}")
            else:
                print(f"Directory not empty, skipped: {path}")
        else:
            print(f"Directory does not exist: {path}")


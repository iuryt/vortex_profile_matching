import warnings
import s3fs
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import os
from matplotlib.path import Path
from datetime import datetime
from glob import glob
import logging
import gsw
import traceback as tb
import os

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from distributed.worker import print, warn
import dask

job_extra_directives = [f"--output=../tmp/slurm-%j.out", f"--error=../tmp/slurm-%j.err"]

cluster = SLURMCluster(cores=1,memory="5GB", walltime='15:00:00', job_extra_directives=job_extra_directives, scheduler_options={"dashboard_address": ":4242"})
cluster.scale(jobs=100)

client = Client(cluster)  # Connect this local process to remote workers



def format_time(ds):
    ds["time"] = ("casts",((ds.time.values-np.datetime64(datetime(1990,1,1))).astype("float")*1e-9)/3600)
    ds["time"].attrs = {"units": "hours since 1990-01-01"}
    return ds
    
def get_encoding(ds):
    encoding = {
        var: (
            {"dtype": "int32"} if ((ds[var].dtype.kind in {"i", "M"})|(var=="casts")) else
            {"dtype": "float32"} if ds[var].dtype.kind == "f"
            else
            {}
        )
        for var in ds.variables
    }
    return encoding

opath = f"../data/processed/concatenated"
os.makedirs(opath, exist_ok=True)

for domain in ["background", "matched"]:
    print(f"\n{domain.upper()}\n")
    for dataset in ["xbt", "apb", "gld", "ctd", "pfl"]:
        print(dataset)
        for variable in tqdm(["Oxygen", "Chlorophyll", "pH", "Nitrate", "Salinity", "Temperature","Oxygen"]):

            fnames = glob(f"../data/processed/{domain}/{dataset}/{variable}/*.nc")

            if len(fnames)>0:
                ds = xr.open_mfdataset(fnames, concat_dim="casts", combine="nested", parallel=True)
                
                ds = format_time(ds)

                ds = add_metadata(ds)
                
                ds.to_netcdf(f"{opath}/{domain}_{dataset}_{variable}.nc", encoding=get_encoding(ds))









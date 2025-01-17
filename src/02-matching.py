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

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from distributed.worker import print, warn
import dask


job_extra_directives = [f"--output=../tmp/slurm-%j.out", f"--error=../tmp/slurm-%j.err"]

cluster = SLURMCluster(cores=1, memory="5GB", walltime='15:00:00', job_extra_directives=job_extra_directives, scheduler_options={"dashboard_address": ":4242"})
cluster.scale(jobs=100)

client = Client(cluster)  # Connect this local process to remote workers


variables = ["Temperature", "Salinity", "Oxygen", "Chlorophyll", "pH", "Nitrate"]

datasets = ["pfl", "ctd", "xbt", "apb", "gld"]

for dataset in datasets:
    path = f"../data/processed/matched/{dataset}"
    os.makedirs(path, exist_ok=True)
    for variable in variables:
        os.makedirs(f"{path}/{variable}", exist_ok=True)

for dataset in datasets:
    path = f"../data/processed/background/{dataset}"
    os.makedirs(path, exist_ok=True)
    for variable in variables:
        os.makedirs(f"{path}/{variable}", exist_ok=True)


def match(
    dataset, year,
    contour = "effective_contour",
    search_size = 10,
    ):

    try:
        lon360to180 = lambda lon: (((lon + 180) % 360) - 180)
        lon180to360 = lambda lon: lon % 360
        
        check_lon360 = lambda lon: (lon>150)&(lon<210)

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

        
        def calculate_Ro(D,rotation):
            umax = np.abs(D.uavg_profile).max("NbSample")
            f = np.abs(gsw.f(D.latitude))
            L = D.speed_radius
            return D.assign(umax=umax, f=f, Ro=rotation*umax/(L*f))
            
        def preprocess(ds,rotation=0):
            params = ["longitude","speed_contour_longitude","effective_contour_longitude"]
            lon180 = {f"{param}":lon360to180(ds[param]) for param in params}
            ds = ds.assign(lon180)
            ds = ds.assign(rotation=rotation*xr.ones_like(ds.amplitude))
            ds = calculate_Ro(ds,rotation)
            return ds
        
        
        base = "../data/processed/meta/META3.2_DT_allsat"
        fnames = glob(f"{base}_Anticyclonic_{year}_*.nc")
        fnames.sort()
        acy = xr.concat([xr.open_dataset(fname).load() for fname in fnames], "obs")
        acy = preprocess(acy,-1).groupby("time")

        fnames = glob(f"{base}_Cyclonic_{year}_*.nc")
        fnames.sort()
        cyc = xr.concat([xr.open_dataset(fname).load() for fname in fnames], "obs")
        cyc = preprocess(cyc,1).groupby("time")
        
        
        fnames = glob(f"../data/processed/wod/{dataset}/*/*{year}*")
        fnames.sort()
        
        wod = []
        for fname in fnames:
            wod.append(xr.open_dataset(fname)[["casts","lon","lat","time"]].sortby("casts").load())
        wod = xr.concat(wod,"casts").drop_duplicates("casts")
        
        # remove the 1770-01-01 entries
        wod = wod.where(wod.time>np.datetime64(datetime(1992,12,31)),drop=True)
        
        wod = wod.groupby(wod.time.dt.floor("1D").rename("time"))
        
        
        mo_profs = []
        for time, profs in wod:
            all_eddies = [cyc[time],acy[time]]
        
            variables = list(cyc[time])
            variables = list(set(variables).difference(set(["time"])))
        
            dummy = np.nan*cyc[time][variables].isel(obs=slice(None,profs.casts.size))
            dummy = dummy.rename(obs="casts")
        
            profs = profs.assign({f"eddy_{var}":dummy[var] for var in variables})
        
            for eddies in [cyc[time],acy[time]]:
                eddies = eddies.where(eddies[f"{contour}_longitude"].std("NbSample")!=0)
        
                plons180 = profs.lon.values
                plons360 = lon180to360(plons180)
                plats = profs.lat.values
        
                elons180 = eddies[f"longitude"].values
                elons360 = lon180to360(elons180)
                elats = eddies[f"latitude"].values
        
                clons180 = eddies[f"{contour}_longitude"].values
                clons360 = lon180to360(clons180)
                clats = eddies[f"{contour}_latitude"].values
        
        
                for pi,(plon180, plon360, plat) in enumerate(zip(plons180, plons360, plats)):
                        
                    if check_lon360(plon360):
                        elons = elons360
                        clons = clons360
                        plon = plon360
                    else:
                        elons = elons180
                        clons = clons180
                        plon = plon180
        
                    around = np.argwhere(
                        (elons>=plon-search_size)&(elons<=plon+search_size)&
                        (elats>=plat-search_size)&(elats<=plat+search_size)
                    ).ravel()
        
        
                    for i,(clon,clat) in enumerate(zip(clons[around],clats[around])):
                        if Path(np.vstack([clon,clat]).T).contains_point((plon,plat)):
                            ci = around[i]
                            for var in variables:
                                profs[f"eddy_{var}"][pi] = eddies[var][ci]
            mo_profs.append(profs)
        mo_profs = xr.concat(mo_profs,"casts")
        
        matched = mo_profs.dropna("casts")
        
        for fname in fnames:
            ofname = fname.split("/")[-1]
            variable = fname.split("/")[-2]
            
            ds = xr.open_dataset(fname)
            
            matched_casts = list(set(ds.casts.values).intersection(set(matched.casts.values)))
            
            if len(matched_casts)>0:
                ds_matched = format_time(xr.merge([ds.sel(casts=matched_casts),matched.sel(casts=matched_casts)]))
                ds_matched.to_netcdf(f"../data/processed/matched/{dataset}/{variable}/{ofname}", encoding=get_encoding(ds_matched))

            background_casts = list(set(ds.casts.values).difference(set(matched.casts.values)))
            
            if len(background_casts)>0:
                ds_background = format_time(ds.sel(casts=background_casts))
                ds_background.to_netcdf(f"../data/processed/background/{dataset}/{variable}/{ofname}", encoding=get_encoding(ds_background))
                
    except Exception as e:
        error_message = ''.join(tb.format_exception(None, e, e.__traceback__))
        return (year,dataset,error_message)



datasets = ["ctd", "xbt", "apb", "gld", "pfl"]
years = np.arange(1993, 2022+1)

objs = []
for dataset in datasets:
    for year in years:
            fnames = glob(f"../data/processed/wod/{dataset}/*/*{year}*")
            if len(fnames)>0:
                objs.append(dask.delayed(match)(
                    dataset, year, contour = "effective_contour", search_size = 10
                ))

results = dask.compute(objs)

error = [r for r in results[0] if r!=None]

check = np.all(np.vstack(error)[:,0]=='2022')
print(check)

######################################################################################

variables = ["Temperature", "Salinity", "Oxygen", "Chlorophyll", "pH", "Nitrate"]

datasets = ["pfl", "ctd", "xbt", "apb", "gld"]

for dataset in datasets:
    for variable in variables:
        for domain in ["matched", "background"]:
            path = f"../data/processed/{domain}/{dataset}/{variable}"  # Construct the path
            if os.path.exists(path):  # Check if the directory exists
                files = os.listdir(path)  # List files in the directory
                if not files:  # If no files exist in the directory
                    os.rmdir(path)  # Remove the directory
                    print(f"Deleted empty directory: {path}")
                else:
                    print(f"Directory not empty, skipped: {path}")
            else:
                print(f"Directory does not exist: {path}")




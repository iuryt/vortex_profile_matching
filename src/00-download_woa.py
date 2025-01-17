import xarray as xr
import numpy as np
import gsw
import os
from tqdm import tqdm

from dask_jobqueue import SLURMCluster
from dask.distributed import Client

# --- Parallel computing setup (optional) ---
print("Setting up parallel computing cluster...")
log_dir = "../tmp"
cluster = SLURMCluster(
    cores=1,
    memory="2GB",
    job_extra_directives=[
        f"--output={log_dir}/slurm-%j.out",
        f"--error={log_dir}/slurm-%j.err"
    ],
    scheduler_options={"dashboard_address": ":4242"}
)
cluster.scale(jobs=50)
client = Client(cluster) 

# --- Data loading ---
print("Loading WOA18 data...")
base_url = "https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/woa"
params = ["temperature", "salinity"]

datasets = []
months = np.arange(1, 13)

for param in tqdm(params):
    urls = [f"{base_url}/{param}/decav/1.00/woa18_decav_{param[0]}{m:02.0f}_01.nc" for m in months]
    ds = xr.open_mfdataset(urls, decode_times=False, parallel=True)

    ds = ds.rename(time="month").assign_coords(month=months)  
    datasets.append(ds)

ds = xr.merge(datasets).load()
ds = ds.rename(depth="z")
print("Data loaded successfully!")

# --- Calculate derived variables ---
print("Calculating derived variables...")
SA = gsw.SA_from_SP(ds.s_an, ds.z, ds.lon, ds.lat)
SA.attrs = {}
SA.name = "absolute_salinity"

CT = gsw.CT_from_t(SA, ds.t_an, ds.z)
CT.attrs = {}
CT.name = "conservative_temperature"

PD = gsw.sigma0(SA, CT) + 1000
PD.name = "potential_density"
print("Calculations complete!")

# --- Create climatology dataset ---
print("Creating climatology dataset...")
clim = xr.merge([SA, CT, PD])

# --- Create directory and save to NetCDF ---
output_dir = "../data/external/woa"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "climatology_woa18.nc")

print("Saving climatology to NetCDF...")
clim.to_netcdf(output_path)
print("Climatology saved successfully!")
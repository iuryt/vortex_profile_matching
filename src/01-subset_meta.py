import xarray as xr
import numpy as np
from tqdm import tqdm

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask

cluster = SLURMCluster(cores=1, memory="5GB")
cluster.scale(jobs=60)    # Deploy ten single-node jobs
client = Client(cluster)  # Connect this local process to remote workers

# load bathymetry
bat = (
    xr.open_dataset("../data/external/gebco/GEBCO_2022_sub_ice_topo.nc")
    .elevation.sel(lon=slice(None,None,10), lat=slice(None,None,10))
).chunk(lon=200,lat=200).load()



for rotation in ["Cyclonic","Anticyclonic"]:
    base = "../data/external/meta/"
    fname = f"META3.2_DT_allsat_{rotation}_long_19930101_20220209.nc"
    eddies = xr.open_dataset(f"{base}/{fname}")
    
    mean_amplitude = eddies.amplitude.chunk(obs=1000000).groupby(eddies.track).mean()

    high_amp = (mean_amplitude.sel(track=eddies.track)>=0.025)
    ind = np.argwhere(high_amp.values).ravel()
    eddies = eddies.isel(obs=ind)
    
    ndays = eddies.track.groupby(eddies.track).count()
    long_eddies = (ndays.sel(track=eddies.track)>=30)
    ind = np.argwhere(long_eddies.values).ravel()
    eddies = eddies.isel(obs=ind)

    eddies = eddies.chunk(obs=300000)

    local_depth = bat.sel(lon=eddies.longitude, lat=eddies.latitude, method="nearest")
    groups = local_depth.groupby(eddies.track)
    origin_local_depth = groups.first()
    n = groups.count(method="map-reduce").values

    where = [[deep]*n[i] for i,deep in enumerate(tqdm((origin_local_depth<-200).values))]
    where = np.hstack(where)
    ind = np.argwhere(where).ravel()

    eddies = eddies.isel(obs=ind)

    eddies.load()

    base = "../data/processed/meta/"

    groups_yr = eddies.groupby("time.year")
    for year,group_yr in tqdm(groups_yr):
        groups_mo = group_yr.groupby("time.month")
        for month, group_mo in groups_mo:
            fname = f"{base}/META3.2_DT_allsat_{rotation}_{year}_{month:02d}.nc"
            encoding={key:dict(_FillValue=9e4) for key in list(group_mo.variables)}
            group_mo.to_netcdf(fname,encoding=encoding)


    del eddies, groups_yr, groups_mo


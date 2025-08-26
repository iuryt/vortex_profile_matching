#%%
import xarray as xr
from dask.diagnostics import ProgressBar
import icechunk
import zarr
from icechunk.xarray import to_icechunk
import os

#%%
zarr.config.set({"async.concurrency": 64})

domains = ["matched","background"]
datasets = ["pfl", "ctd", "apb", "xbt", "gld"]
variables = ["Temperature", "Salinity", "Oxygen", "Nitrate", "Chlorophyll", "pH"]

#%%

for domain in domains:
    for dataset in datasets:
        for variable in variables:
            
            # 1) abrir o NetCDF
            SRC = f"../data/external/{domain.title()}/{domain}_{dataset}_{variable}.nc"   # caminho local do arquivo

            # check if the file exists

            if not os.path.exists(SRC):
                # skip loop iteration if file does not exist
                print(f"File {SRC} does not exist. Skipping...")
                continue
            else:
                print(f"Processing file: {SRC}")

            ds = xr.open_dataset(SRC).load()

            # garantir chunking por cast (1) e todos os níveis em cada chunk
            nlev = int(ds.sizes["levels"])  # seus dims são casts x levels
            ds = ds.chunk({"casts": 1000, "levels": nlev})

            # 3) criar/abrir o repositório Icechunk no S3
            storage = icechunk.s3_storage(
                bucket="iuryt-shared",
                prefix=f"icechunk/ocean/vortex-profiles/{domain.title()}/{dataset}_{variable}",
                region="us-west-2",
            )

            try:
                repo = icechunk.Repository.open(storage)
            except Exception:
                repo = icechunk.Repository.create(storage)

            # escrever (commitar) usando os chunks definidos acima
            sess = repo.writable_session("main")

            with ProgressBar():
                to_icechunk(ds, sess)
            snap = sess.commit("First commit. Chunked by casts.")
            print("Snapshot written:", snap)


# %%

#%%
# pip install -U xarray netCDF4 zarr s3fs icechunk
import xarray as xr
import icechunk
import zarr

#%%
# (opcional) mais paralelismo nas requisições assíncronas
zarr.config.set({"async.concurrency": 64})

# 1) abrir o NetCDF
SRC = "matched_pfl_Temperature.nc"   # caminho local do arquivo
ds = xr.open_dataset(SRC, engine="netcdf4", decode_cf=True)

# 2) manter só as variáveis necessárias (ajuste se quiser mais)
keep = [v for v in ["temperature", "z", "lat", "lon", "time"] if v in ds]
if keep:
    ds = ds[keep]

# garantir chunking por cast (1) e todos os níveis em cada chunk
nlev = int(ds.dims["levels"])  # seus dims são casts x levels
ds = ds.chunk({"casts": 1, "levels": nlev})

# 3) criar/abrir o repositório Icechunk no S3
storage = icechunk.s3_storage(
    bucket="iuryt-shared",
    prefix="icechunk/ocean/vortex-profiles",
    from_env=True,            # usa suas credenciais AWS do ambiente/perfil
)
try:
    repo = icechunk.Repository.open(storage)
except Exception:
    repo = icechunk.Repository.create(storage)

# escrever (commitar) usando os chunks definidos acima
sess = repo.writable_session("main")
from icechunk.xarray import to_icechunk
to_icechunk(ds, sess)
snap = sess.commit("import matched_pfl_Temperature.nc chunk=(casts=1, levels=all)")
print("Snapshot gravado:", snap)

# 4) leitura de verificação: carrega um perfil (cast) de volta
ro = repo.readonly_session(snapshot_id=snap)
ds2 = xr.open_zarr(ro.store, consolidated=False, chunks={})
perfil0 = ds2.isel(casts=0)[["temperature", "z", "lat", "lon", "time"]].load()
print(perfil0)

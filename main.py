import time
from pathlib import Path

import click
import xarray as xr

chunks = {"chunksizes": [64, 16, 16]}

print("started")


def chunk_netcdf_folder(input_folder, output_folder, limit: int = -1):
    output_folder = Path(output_folder)
    output_folder.mkdir(exist_ok=True, parents=True)
    datasets = list(Path(input_folder).glob("*.nc"))

    for n, dataset in enumerate(datasets):
        if 0 < limit <= n:
            break

        t = time.perf_counter()

        # to keep original 'packing' use mask_and_scale=False
        ds = xr.open_dataset(
            dataset, mask_and_scale=False, decode_times=False, chunks={"time": 512}
        )
        encoding = {var: chunks for var in ds.data_vars}
        encoding["time"] = {"dtype": "single"}

        output_filename = output_folder / dataset.name
        ds.to_netcdf(output_filename, encoding=encoding)

        print(f"{time.perf_counter() - t:.2f} seconds", dataset.name)
        print(f"{n + 1} of {len(datasets)} datasets computed")


@click.command()
@click.argument("input-folder", type=click.Path(file_okay=False))
@click.argument("output-folder", type=click.Path(file_okay=False))
@click.option("--limit", type=int, default=-1, help="Process only this number of files")
def main(input_folder, output_folder, limit):
    """Chunk a folder of netcdf files to an output directory."""
    chunk_netcdf_folder(input_folder, output_folder, limit)


if __name__ == "__main__":
    main()

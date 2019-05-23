import time
from pathlib import Path

import click
import xarray as xr

chunks = {"chunksizes": [256, 16, 16]}

print("started")


def chunk_netcdf_folder(input_folder, output_folder, limit: int = -1):
    output_folder = Path(output_folder)
    output_folder.mkdir(exist_ok=True, parents=True)
    datasets = list(Path(input_folder).glob("*.nc"))

    for n, dataset in enumerate(datasets):
        if 0 < limit <= n:
            break
        output_filename = output_folder / dataset.name
        chunk_netcdf_file(dataset, output_filename)
        print(f"{n + 1} of {len(datasets)} datasets computed")

def chunk_netcdf_file(input_file_path, output_file_path):
    t = time.perf_counter()

    # to keep original 'packing' use mask_and_scale=False
    ds = xr.open_dataset(
        input_file_path, mask_and_scale=False, decode_times=False, chunks={"time": 512}
    )
    encoding = {var: chunks for var in ds.data_vars}
    encoding["time"] = {"dtype": "single"}

    ds.to_netcdf(output_file_path, encoding=encoding)
    print(f"{time.perf_counter() - t:.2f} seconds", input_file_path.name)

@click.group()
def main():
    pass

@main.command()
@click.argument("input-folder", type=click.Path(file_okay=False))
@click.argument("output-folder", type=click.Path(file_okay=False))
@click.option("--limit", type=int, default=-1, help="Process only this number of files")
def directory(input_folder, output_folder, limit):
    """Chunk a folder of netcdf files to an output directory."""
    chunk_netcdf_folder(input_folder, output_folder, limit)

@main.command()
@click.argument("input-file", type=click.Path(exists=True, dir_okay=False))
@click.argument("output-file", type=click.Path(dir_okay=False))
def file(input_file, output_file):
    """Chunk a single netcdf file to an output file."""
    chunk_netcdf_file(Path(input_file),output_file)


if __name__ == "__main__":
    main()

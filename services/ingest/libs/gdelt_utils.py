from __future__ import annotations

import zipfile
from hashlib import sha256
from pathlib import Path

import aiofiles
import aiohttp


class ManifestStatusError(Exception): ...


async def download_file(url: str, path: str, chunk_size: int = 1024, test: bool = False) -> None:
    """
    Masterfile URL: http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()

            async with aiofiles.open(path, "wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    if chunk:
                        await f.write(chunk)
                    if test:
                        break


def pickle_and_dump(data, path):
    import pickle

    with open(path, "wb") as f:
        pickle.dump(data, f)


def parse_line(line: str) -> tuple[str, str, str, str, str, str] | None:
    parts = line.split()
    if len(parts) == 3:
        url = parts[2].strip()
        filename = url.split("/")[-1]
        return (
            parts[0].strip(),  # size
            parts[1].strip(),  # hash
            url,
            ".".join(filename.split(".")[:-2]),  # basename
            filename.split(".")[0],  # dt
            filename.split(".")[-2],  # format
        )


async def read_meta(path):
    async with aiofiles.open(path, "r") as f:
        while line := await f.readline():
            yield line


# zipfile is blocking. use this with asyncio.to_thread
def unzip_csv(source_file: Path, target_file: Path):
    """
    Extracts a csv.zip file to a csv file
    """
    with zipfile.ZipFile(source_file) as zf:
        zf.extractall(target_file)


def compute_hash(path: Path):
    h = sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

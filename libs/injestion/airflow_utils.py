import aiofiles
import aiohttp


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

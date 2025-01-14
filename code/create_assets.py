import asyncio
import concurrent.futures
import os

from codeocean import CodeOcean
from codeocean.components import UserPermissions, EveryoneRole, UserRole
from codeocean.data_asset import AWSS3Source, ComputationSource, DataAssetParams, Permissions, Source, Target

async def create_and_wait(client, data_asset_params):
    """
    Asynchronously creates and waits for a data asset.

    Args:
        client: The client object.
        data_asset_params: Parameters for creating the data asset.

    Returns:
        The created data asset.
    """
    data_asset = await asyncio.to_thread(client.data_assets.create_data_asset, data_asset_params)
    print(f"Creating Data Asset: {data_asset_params}\n")
    data_asset = await asyncio.to_thread(client.data_assets.wait_until_ready, data_asset)
    print(f"Data Asset - {data_asset.name} - creation complete\nID: {data_asset.id}\n")
    return data_asset

async def main(client, data_asset_params_list):
    """
    Asynchronously creates and waits for multiple data assets concurrently.

    Args:
        client: The client object.
        data_asset_params_list: A list of dictionaries containing parameters for each data asset.

    Returns:
        A list of created data assets.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        tasks = [
            asyncio.ensure_future(create_and_wait(client, params))
            for params in data_asset_params_list
        ]
        return await asyncio.gather(*tasks)

if __name__ == "__main__":

    co_api_token = os.getenv("CUSTOM_KEY")
    co_domain = "https://my-organization.codeocean.com" # replace with your Code Ocean domain
    client=CodeOcean(domain=co_domain,token=co_api_token)

    reads_data_asset_params = DataAssetParams(
        name="Pipeline Training: NGS Reads",
        description="Paired end reads from GSE157194 patient 1",
        mount="reads",
        tags=["fastq", "genomics", "SDK"],
        source=Source(
            aws=AWSS3Source(
                bucket="codeocean-public-data",
                prefix=f"example_datasets/rna-seq-tutorial/GSE157194_reads/",
                public=True,
                keep_on_external_storage=False
                )))
    
    gtf_data_asset_params = DataAssetParams(
        name="Pipeline Training: GRCh38 Release 21 GTF",
        description="Comprehensive gene annotation on the reference chromosomes only.",
        mount="annotation",
        tags=["gtf", "genomics", "SDK"],
        source=Source(
            aws=AWSS3Source(
                bucket="codeocean-public-data",
                prefix=f"example_datasets/GRCh38_GTF/",
                public=True,
                keep_on_external_storage=False
                )))
    
    star_data_asset_params = DataAssetParams(
        name="Pipeline Training: STAR GRCh38 GENCODE Release 21 Index",
        description="Created with STAR 2.7.10a from GENCODE Release 21 (GRCh38) assembly with comprehensive gene annotation.",
        mount="star_index",
        tags=["STAR", "genomics", "SDK", "index"],
        source=Source(
            aws=AWSS3Source(
                bucket="codeocean-public-data",
                prefix=f"example_datasets/STAR_GRCh38_GENCODE_Release_21_Index/star_index/",
                public=True,
                keep_on_external_storage=False
                )))

    data_asset_params_list = [
        reads_data_asset_params,
        gtf_data_asset_params,
        star_data_asset_params,
    ]

    created_assets = asyncio.run(main(client, data_asset_params_list))

    update_permissions = Permissions(
        everyone=EveryoneRole(value="viewer"),
        share_assets=True
    )    

    for asset in created_assets:
        client.data_assets.update_permissions(
            data_asset_id=asset.id,
            permissions=update_permissions
        )
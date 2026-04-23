from pathlib import Path
from uuid import UUID

from backend.db.dal import DALAssets, DAOAssetsUpdate, safe_commit
from backend.db.data_models import AssetUploadStatus
from backend.db.session.factory import AsyncSessionFactory
from backend.lib.asset_manager.base import AssetManager, AssetStorageKey
from backend.lib.types.asset import Asset
from backend.lib.utils.common import none_throws
from backend.lib.utils.web_requests import async_tempdir

from .remote import RemoteJobProcessor
from .types import (
    PostProcessUploadedAssetsInputPayload,
    PostProcessUploadedAssetsOutputPayload,
)
from .utils.compress_upload_mixin import AssetKeyType, CompressUploadMixin
from .utils.types import CompressionTier
from .utils.vips import ImageProcessingLibrary

MIN_FREE_DISK_BYTES = 100 * 1024 * 1024  # 100MB


class RemotePostProcessUploadedAssets(
    RemoteJobProcessor[
        PostProcessUploadedAssetsInputPayload, PostProcessUploadedAssetsOutputPayload
    ],
    CompressUploadMixin,
):
    def __init__(
        self,
        job_id: UUID,
        asset_manager: AssetManager,
        db_session_factory: AsyncSessionFactory,
    ) -> None:
        self._image_lib = ImageProcessingLibrary(
            max_concurrent=1,
        )
        super().__init__(
            job_id,
            asset_manager,
            db_session_factory,
        )

    async def process(
        self, input_payload: PostProcessUploadedAssetsInputPayload
    ) -> PostProcessUploadedAssetsOutputPayload:
        compress_upload_succeeded_uuids: list[UUID] = []
        compress_upload_failed_uuids: list[UUID] = []
        rejected_mime_uuids: list[UUID] = []  # FIXME

        async with async_tempdir() as root_dir:
            try:
                should_abort, error_message = self._sanity_check_free_storage(root_dir)
                if should_abort:
                    raise Exception(error_message)

                # Mark as begin processing
                async with self.db_session_factory.new_session() as db_session:
                    async with safe_commit(
                        db_session,
                        context="processing status DB update",
                        raise_on_fail=False,
                    ):
                        await DALAssets.update_many_by_ids(
                            db_session,
                            {
                                asset_id: DAOAssetsUpdate(
                                    upload_status=AssetUploadStatus.PROCESSING,
                                )
                                for asset_id in input_payload.asset_ids
                            },
                        )

                    asset_daos = await DALAssets.get_by_ids(
                        db_session, input_payload.asset_ids
                    )

                # Begin compressing
                asset_uuid_orig_key_map: dict[UUID, AssetStorageKey] = {
                    dao.id: none_throws(dao.asset_key_original) for dao in asset_daos
                }
                orig_keys_asset_uuid_map: dict[AssetStorageKey, UUID] = {
                    v: k for k, v in asset_uuid_orig_key_map.items()
                }
                download_results: dict[
                    AssetStorageKey, Asset | Exception
                ] = await self.asset_manager.download_files_batched(
                    [
                        (key, root_dir / Path(key).name)
                        for key in asset_uuid_orig_key_map.values()
                    ]
                )

                # original path -> UUID
                media_paths_asset_uuids_map: dict[Path, UUID] = {
                    none_throws(asset.cached_local_path): orig_keys_asset_uuid_map[
                        storage_key
                    ]
                    for storage_key, asset in download_results.items()
                    if isinstance(asset, Asset)
                }

                # orig path -> (success, compressed path)
                compressed_result_llm: dict[
                    Path, tuple[bool, Path | None]
                ] = await self._image_lib.compress_by_tier_on_thread(
                    input_paths=list(media_paths_asset_uuids_map.keys()),
                    output_dir=root_dir,
                    format="jpeg",
                    tier=CompressionTier.LLM,
                    strip_metadata=False,
                )
                compressed_result_highend_display: dict[
                    Path, tuple[bool, Path | None]
                ] = await self._image_lib.compress_by_tier_on_thread(
                    input_paths=list(media_paths_asset_uuids_map.keys()),
                    output_dir=root_dir,
                    format="jpeg",
                    tier=CompressionTier.HIGH_END_DISPLAY,
                    strip_metadata=False,
                )

                # compressed path -> (compressed asset key, orig path, tier)
                succeeded_compressed_path_asset_keys_map: dict[
                    Path, tuple[AssetStorageKey, Path, AssetKeyType]
                ] = dict()

                compressed_results_in_tiers: list[
                    tuple[dict[Path, tuple[bool, Path | None]], AssetKeyType]
                ] = [
                    (compressed_result_llm, "asset_key_llm"),
                    (compressed_result_highend_display, "asset_key_display"),
                ]

                for res, tier in compressed_results_in_tiers:
                    for orig_path, (success, compressed_path) in res.items():
                        if not success:
                            continue

                        compressed_path = none_throws(compressed_path)
                        succeeded_compressed_path_asset_keys_map[compressed_path] = (
                            self.asset_manager.mint_asset_key_for_presigned_slots(
                                input_payload.user_id, compressed_path.name
                            ),
                            orig_path,
                            tier,
                        )

                # Begin uploading
                upload_results = await self.asset_manager.upload_files_batched(
                    [
                        (compressed_path, compressed_asset_key)
                        for compressed_path, (
                            compressed_asset_key,
                            _,
                            _,
                        ) in succeeded_compressed_path_asset_keys_map.items()
                    ]
                )
                succeeded_uploaded_compressed_paths = [
                    p for p in upload_results if isinstance(upload_results[p], Asset)
                ]

                # Aggregate results and write to DB
                writes: dict[UUID, DAOAssetsUpdate] = dict()
                for compressed_path in succeeded_uploaded_compressed_paths:
                    (compressed_asset_key, original_path, tier) = (
                        succeeded_compressed_path_asset_keys_map[compressed_path]
                    )
                    uuid = media_paths_asset_uuids_map[original_path]
                    if uuid not in writes:
                        writes[uuid] = DAOAssetsUpdate()
                    if tier == "asset_key_display":
                        writes[uuid].asset_key_display = compressed_asset_key
                    elif tier == "asset_key_llm":
                        writes[uuid].asset_key_llm = compressed_asset_key
                    else:
                        raise Exception("Invalid compression tier specified")

                for uuid, dao in writes.items():
                    if (
                        dao.asset_key_display is not None
                        and dao.asset_key_llm is not None
                    ):
                        dao.upload_status = AssetUploadStatus.READY
                        compress_upload_succeeded_uuids.append(uuid)
                    else:
                        dao.upload_status = AssetUploadStatus.PROCESSING_FAILED
                        compress_upload_failed_uuids.append(uuid)

                for uuid in asset_uuid_orig_key_map.keys():
                    if uuid not in writes:
                        writes[uuid] = DAOAssetsUpdate(
                            upload_status=AssetUploadStatus.PROCESSING_FAILED
                        )
                        compress_upload_failed_uuids.append(uuid)

                async with self.db_session_factory.new_session() as db_session:
                    async with safe_commit(
                        db_session,
                        context="persisting processed asset keys",
                        raise_on_fail=True,
                    ):
                        await DALAssets.update_many_by_ids(
                            db_session,
                            writes,
                        )

            except Exception as e:
                async with self.db_session_factory.new_session() as db_session:
                    async with safe_commit(
                        db_session,
                        context="upload failed status DB update",
                        raise_on_fail=False,
                    ):
                        await DALAssets.update_many_by_ids(
                            db_session,
                            {
                                asset_id: DAOAssetsUpdate(
                                    upload_status=AssetUploadStatus.PROCESSING_FAILED,
                                )
                                for asset_id in input_payload.asset_ids
                            },
                        )
                raise e

        return PostProcessUploadedAssetsOutputPayload(
            job_id=self.job_id,
            assets_post_process_succeeded=compress_upload_succeeded_uuids,
            assets_post_process_failed=compress_upload_failed_uuids,
            assets_rejected_invalid_mime=rejected_mime_uuids,
        )

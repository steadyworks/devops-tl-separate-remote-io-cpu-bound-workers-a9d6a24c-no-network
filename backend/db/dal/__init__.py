from backend.db.data_models import (
    DAOAssets,
    DAOJobEvents,
    DAOJobs,
    DAOPages,
    DAOPagesAssetsRel,
    DAOPhotobookBookmarks,
    DAOPhotobooks,
    DAOUsers,
)

from .assets import DALAssets
from .base import (
    AsyncPostgreSQLDAL,
    FilterOp,
    InvalidFilterFieldError,
    OrderDirection,
    safe_commit,
)
from .job_events import DALJobEvents
from .schemas import (
    DAOAssetsCreate,
    DAOAssetsUpdate,
    DAOJobEventsCreate,
    DAOJobsCreate,
    DAOJobsUpdate,
    DAOPagesAssetsRelCreate,
    DAOPagesAssetsRelUpdate,
    DAOPagesCreate,
    DAOPagesUpdate,
    DAOPhotobookBookmarksCreate,
    DAOPhotobookBookmarksUpdate,
    DAOPhotobooksCreate,
    DAOPhotobooksUpdate,
    DAOUsersCreate,
    DAOUsersUpdate,
)


class DALJobs(AsyncPostgreSQLDAL[DAOJobs, DAOJobsCreate, DAOJobsUpdate]):
    model = DAOJobs


class DALPages(AsyncPostgreSQLDAL[DAOPages, DAOPagesCreate, DAOPagesUpdate]):
    model = DAOPages


class DALPagesAssetsRel(
    AsyncPostgreSQLDAL[
        DAOPagesAssetsRel, DAOPagesAssetsRelCreate, DAOPagesAssetsRelUpdate
    ]
):
    model = DAOPagesAssetsRel


class DALPhotobooks(
    AsyncPostgreSQLDAL[DAOPhotobooks, DAOPhotobooksCreate, DAOPhotobooksUpdate]
):
    model = DAOPhotobooks


class DALUsers(AsyncPostgreSQLDAL[DAOUsers, DAOUsersCreate, DAOUsersUpdate]):
    model = DAOUsers


class DALPhotobookBookmarks(
    AsyncPostgreSQLDAL[
        DAOPhotobookBookmarks, DAOPhotobookBookmarksCreate, DAOPhotobookBookmarksUpdate
    ]
):
    model = DAOPhotobookBookmarks


__all__ = [
    # DALs
    "DALAssets",
    "DALJobs",
    "DALJobEvents",
    "DALPages",
    "DALPagesAssetsRel",
    "DALPhotobooks",
    "DALPhotobookBookmarks",
    # DAL base
    "AsyncPostgreSQLDAL",
    "FilterOp",
    "InvalidFilterFieldError",
    "OrderDirection",
    # ORM objects
    "DAOAssets",
    "DAOJobs",
    "DAOJobEvents",
    "DAOPages",
    "DAOPagesAssetsRel",
    "DAOPhotobooks",
    "DAOUsers",
    "DAOPhotobookBookmarks",
    # Schemas
    "DAOAssetsCreate",
    "DAOAssetsUpdate",
    "DAOJobsCreate",
    "DAOJobsUpdate",
    "DAOJobEventsCreate",
    # "DAOJobEventsUpdate",   # Updating job events is not allowed, as it's an append-only log trail
    "DAOPagesCreate",
    "DAOPagesUpdate",
    "DAOPagesAssetsRelCreate",
    "DAOPagesAssetsRelUpdate",
    "DAOPhotobooksCreate",
    "DAOPhotobooksUpdate",
    "DAOUsersCreate",
    "DAOUsersUpdate",
    "DAOPhotobookBookmarksUpdate",
    "DAOPhotobookBookmarksCreate",
    # Utils
    "safe_commit",
]

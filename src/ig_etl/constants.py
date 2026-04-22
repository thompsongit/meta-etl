IG_GRAPH_BASE = "https://graph.instagram.com"
DEFAULT_GRAPH_BASE = IG_GRAPH_BASE
DEFAULT_GRAPH_VERSION = "v24.0"
STREAM_NAME = "ig_media_insights_comments"

MEDIA_FIELDS_CANDIDATES = [
    ",".join(
        [
            "id",
            "caption",
            "timestamp",
            "media_type",
            "media_product_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "username",
            "is_comment_enabled",
            "comments_count",
            "like_count",
        ]
    ),
    ",".join(
        [
            "id",
            "caption",
            "timestamp",
            "media_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "username",
            "is_comment_enabled",
            "comments_count",
            "like_count",
        ]
    ),
    ",".join(
        [
            "id",
            "caption",
            "timestamp",
            "media_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "username",
            "comments_count",
            "like_count",
        ]
    ),
    ",".join(
        [
            "id",
            "caption",
            "timestamp",
            "media_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "username",
        ]
    ),
]

USER_INSIGHT_CANDIDATES = [
    {"metric": "views,reach,total_interactions", "period": "day"},
    {"metric": "accounts_engaged,views,reach", "period": "day"},
    {"metric": "follower_count", "period": "day"},
    {"metric": "impressions,reach", "period": "day"},
]

MEDIA_INSIGHT_CANDIDATES = [
    {"metric": "views,reach,likes,comments,saved,shares", "period": "lifetime"},
    {"metric": "reach,likes,comments,saved,shares", "period": "lifetime"},
    {"metric": "views,likes,comments,shares", "period": "lifetime"},
    {"metric": "impressions,reach,saved,likes,comments,shares", "period": "lifetime"},
]

COMMENT_FIELDS_CANDIDATES = [
    "id,text,timestamp,username,like_count,hidden,parent_id",
    "id,text,timestamp,username,like_count,parent_id",
    "id,text,timestamp,username,parent_id",
]

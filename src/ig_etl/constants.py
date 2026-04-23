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

CHILD_MEDIA_FIELDS_CANDIDATES = [
    "id,media_type,media_product_type,permalink,media_url,thumbnail_url,timestamp",
    "id,media_type,permalink,media_url,thumbnail_url,timestamp",
    "id,media_type,permalink,timestamp",
]

STORY_FIELDS_CANDIDATES = [
    "id,media_type,media_product_type,permalink,media_url,thumbnail_url,timestamp",
    "id,media_type,permalink,media_url,thumbnail_url,timestamp",
    "id,media_type,permalink,timestamp",
]

TAG_FIELDS_CANDIDATES = [
    "id,media_type,permalink,caption,timestamp",
    "id,media_type,permalink,timestamp",
    "id,media_type,timestamp",
]

MENTIONED_MEDIA_FIELDS_CANDIDATES = [
    "id,media_type,permalink,caption,timestamp",
    "id,media_type,permalink,timestamp",
    "id,media_type,timestamp",
]

COMMENT_REPLY_FIELDS_CANDIDATES = [
    "id,text,timestamp,username,like_count,hidden",
    "id,text,timestamp,username,like_count",
    "id,text,timestamp,username",
]

HASHTAG_MEDIA_FIELDS_CANDIDATES = [
    "id,media_type,permalink,caption,timestamp",
    "id,media_type,permalink,timestamp",
    "id,media_type,timestamp",
]

BUSINESS_DISCOVERY_PROFILE_FIELDS_CANDIDATES = [
    "id,username,name,biography,website,followers_count,follows_count,media_count,media.limit(50){id,media_type,media_product_type,permalink,caption,timestamp}",
    "id,username,name,biography,website,followers_count,follows_count,media_count",
    "id,username,name,followers_count,follows_count,media_count",
]

CONVERSATION_FIELDS_CANDIDATES = [
    "id,updated_time,participants",
    "id,updated_time",
    "id",
]

MESSAGE_FIELDS_CANDIDATES = [
    "id,from,to,message,created_time,is_echo",
    "id,from,to,text,created_time,is_echo",
    "id,from,message,created_time",
    "id,from,text,created_time",
]

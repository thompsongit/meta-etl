from __future__ import annotations

DEFAULT_GRAPH_BASE = "https://graph.facebook.com"
DEFAULT_GRAPH_VERSION = "v25.0"

STREAM_NAME = "fb_page_analytics"
POSTS_CURSOR_STREAM = "fb_page_posts"
COMMENTS_CURSOR_STREAM = "fb_post_comments"

PAGE_FIELDS_CANDIDATES = [
    (
        "id,name,username,about,description,category,link,fan_count,"
        "followers_count,is_published,verification_status,overall_star_rating,"
        "rating_count"
    ),
    (
        "id,name,username,about,description,category,link,fan_count,"
        "followers_count,is_published,verification_status"
    ),
    "id,name,category,link,fan_count,followers_count",
]

POST_FIELDS_CANDIDATES = [
    (
        "id,message,story,status_type,type,created_time,updated_time,permalink_url,"
        "full_picture,shares,reactions.summary(true).limit(0),"
        "comments.summary(true).limit(0),attachments"
    ),
    "id,message,story,status_type,type,created_time,updated_time,permalink_url,full_picture,shares",
    "id,message,created_time,updated_time,permalink_url",
]

COMMENT_FIELDS_CANDIDATES = [
    "id,from{id,name},message,created_time,like_count,comment_count,parent{id},is_hidden",
    "id,from{id,name},message,created_time,like_count,parent{id}",
    "id,message,created_time",
]

PAGE_INSIGHT_METRICS = [
    "page_media_view",
    "page_post_engagements",
    "page_follows",
    "page_impressions",
    "page_impressions_unique",
    "page_views_total",
    "page_total_actions",
]

POST_INSIGHT_METRICS = [
    "post_media_view",
    "post_impressions", #probably another breaking change in the new API v25.0
    "post_impressions_unique",
    "post_clicks",
    "post_reactions_by_type_total",
]

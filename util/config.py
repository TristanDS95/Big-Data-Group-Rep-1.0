"""Configuration and constants for CircleNet data generators."""

HOBBIES = [
    "reading", "gardening", "cooking", "hiking", "painting",
    "photography", "cycling", "swimming", "gaming", "knitting",
    "running", "yoga", "chess", "fishing", "camping",
    "drawing", "music", "dancing", "traveling", "birdwatching",
    "woodworking", "pottery", "tennis", "golf", "skiing",
]

JOB_TITLES = [
    "Software Engineer", "Data Scientist", "Project Manager",
    "Marketing Director", "Sales Representative", "HR Specialist",
    "Financial Analyst", "Design Engineer", "Research Assistant",
    "Operations Manager", "Account Executive", "Quality Analyst",
]

FOLLOW_REASONS = [
    "shared interests", "mutual friends", "interesting content",
    "professional networking", "local community", "hobby groups",
    "similar background", "recommended by friends", "common goals",
    "industry connections", "creative work", "helpful posts",
]

DESCRIPTION_PHRASES = [
    "Followed for {reason}",
    "Connected due to {reason}",
    "Started following for {reason}",
]

ActionType = [
    "view", "like", "comment", "share", "follow",
    "unfollow", "message", "bookmark", "mention", "react",
]
Null = None

ActionType2 = [
    ( "viewed", "poked "),("viewed","lefft a note"),("viewed","liked"),
    ("viewed","loved"),("viewed","disliked"),("viewed","sad"),( "viewed", "poked "),
    ("viewed","shared"),("viewed","saved"),("viewed","followed"),("viewed","direct message"),
    ("viewed", Null),]

SMALL_CONFIG = {
    "num_pages": 100,
    "num_follows": 500,
}

FULL_CONFIG = {
    "num_pages": 200_000,
    "num_follows": 10_000_000,
}

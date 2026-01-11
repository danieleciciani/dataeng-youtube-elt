SELECT COUNT(*) 
FROM {{ ref('yt-video') }} 
WHERE "likecount" > "viewcount"
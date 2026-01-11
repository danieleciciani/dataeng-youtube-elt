SELECT COUNT(*) 
FROM {{ ref('yt-video') }} 
WHERE "commentcount" > "viewcount"
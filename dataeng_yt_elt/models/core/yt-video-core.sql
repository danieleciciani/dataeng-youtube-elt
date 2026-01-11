/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml
    
    # each dbt run --model yt-video-core overraride the past execution

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

select *, '12345' as mockfield
from {{ ref('yt-video') }}



SELECT * 
FROM {{ ref("stg_customers") }}  AS customers
LEFT JOIN {{ source("main","locations") }} AS locations 
ON locations.last_name = customers.last_name
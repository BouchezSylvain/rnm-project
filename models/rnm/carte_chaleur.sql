{{ config(materialized="view") }}

with
    source_data as (
        select
            categorie,
            produit,
            concat(
                format_date("%m", date_cotation),
                " - ",
                format_date("%b", date_cotation)
            ) as month,
            round(avg(produit_prix_cotation), 2) as avg_price,
            round(max(produit_prix_cotation), 2) as max_price,
            round(min(produit_prix_cotation), 2) as min_price,
        from {{ ref("produit_cotation") }}
        group by produit, categorie, month
        order by month, produit
    )

select *
from source_data

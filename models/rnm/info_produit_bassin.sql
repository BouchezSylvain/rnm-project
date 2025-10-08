{{ config(materialized="view") }}

with
    source_data as (
        select
            categorie,
            bassin,
            produit,
            date_cotation,
            round(avg(produit_prix_cotation), 2) as moyenne_produit_prix_cotation,
            round(min(produit_prix_cotation), 2) as prix_min,
            array_agg(marche.libelle order by produit_prix_cotation asc limit 1)[
                offset(0)
            ] as marche_min,
            round(
                avg(produit_variation_prix_mois_precedent), 2
            ) as moyenne_produit_variation_prix_mois_precedent
        from {{ ref("produit_cotation") }}
        join {{ source("rnm", "marche") }} on produit_cotation.marche = marche.libelle
        where bassin is not null
        group by produit, bassin, date_cotation, categorie
        having moyenne_produit_prix_cotation is not null
        order by produit, date_cotation
    )

select *
from source_data

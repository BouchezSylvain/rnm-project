{{ config(materialized="view") }}

with
    source_data as (
        select
            bassin,
            categorie,
            produit,
            date_cotation,
            marche.libelle,
            marche.adresse,
            round(avg(produit_prix_cotation), 2) as prix_moyen
        from {{ ref("produit_cotation") }}
        join {{ source("rnm", "marche") }} on produit_cotation.marche = marche.libelle
        where marche.libelle like "%MIN%" and bassin is not null
        group by
            produit, bassin, date_cotation, categorie, marche.libelle, marche.adresse
        having prix_moyen is not null
    )

select *
from source_data

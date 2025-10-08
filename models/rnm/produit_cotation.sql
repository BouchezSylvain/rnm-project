{{ config(materialized="table") }}

with
    produit_cotation as (
        select
            produit,
            categorie,
            marche,
            date_cotation,
            round(avg(prix_cotation), 2) as produit_prix_cotation
        from {{ ref("libelle_cotation") }}
        group by produit, categorie, date_cotation, marche
    ),
    produit_cotation_mois_precedent as (
        select
            *,
            lag(produit_prix_cotation) over (
                partition by categorie, produit, marche order by date_cotation
            ) as produit_prix_mois_precedent,
        from produit_cotation
    ),
    variation_cotation_mois_precedent as (
        select
            *,
            round(
                produit_prix_mois_precedent - produit_prix_cotation, 2
            ) as produit_variation_prix_mois_precedent,
            round(
                (produit_prix_mois_precedent - produit_prix_cotation)
                / produit_prix_mois_precedent
                * 100,
                2
            ) as produit_variation_pourcentage_prix_mois_precedent
        from produit_cotation_mois_precedent
    ),
    source_data as (select * from variation_cotation_mois_precedent)

select *
from source_data

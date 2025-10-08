{{ config(materialized="table") }}

-- The first month ie the subscription month, we bill the price_per_month "au prorata"
-- des jours restants
with
    all_csv as (
        select
            stade as stade,
            `Marché` as marche,
            `Libellé` as libelle,
            `Unité` as unite,
            cast(`Janvier` as int64) as `janvier`,
            cast(`Février` as int64) as `fevrier`,
            cast(`Mars` as int64) as `mars`,
            cast(`Avril` as int64) as `avril`,
            cast(`Mai` as int64) as `mai`,
            cast(`Juin` as int64) as `juin`,
            cast(`Juillet` as int64) as `juillet`,
            cast(`Août` as int64) as `aout`,
            cast(`Septembre` as int64) as `septembre`,
            cast(`Octobre` as int64) as `octobre`,
            cast(`Novembre` as int64) as `novembre`,
            cast(`Décembre` as int64) as `decembre`,
            "Agrumes" as categorie,
            2023 as annee
        from {{ source("rnm", "agrumes_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64),
            cast(`Février` as int64),
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Agrumes" as categorie,
            2024 as annee
        from {{ source("rnm", "agrumes_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Agrumes" as categorie,
            2025 as annee
        from {{ source("rnm", "agrumes_2025") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Baies et petits fruits" as categorie,
            2023 as annee
        from {{ source("rnm", "baies_petits_fruits_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Baies et petits fruits" as categorie,
            2024 as annee
        from {{ source("rnm", "baies_petits_fruits_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Baies et petits fruits" as categorie,
            2025 as annee
        from {{ source("rnm", "baies_petits_fruits_2025") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à noyau" as categorie,
            2023 as annee
        from {{ source("rnm", "fruits_a_noyau_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à noyau" as categorie,
            2024 as annee
        from {{ source("rnm", "fruits_a_noyau_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à noyau" as categorie,
            2025 as annee
        from {{ source("rnm", "fruits_a_noyau_2025") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à pépins" as categorie,
            2023 as annee
        from {{ source("rnm", "fruits_a_pepins_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à pépins" as categorie,
            2024 as annee
        from {{ source("rnm", "fruits_a_pepins_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits à pépins" as categorie,
            2025 as annee
        from {{ source("rnm", "fruits_a_pepins_2025") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits exotiques" as categorie,
            2023 as annee
        from {{ source("rnm", "fruits_exotiques_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits exotiques" as categorie,
            2024 as annee
        from {{ source("rnm", "fruits_exotiques_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits exotiques" as categorie,
            2025 as annee
        from {{ source("rnm", "fruits_exotiques_2025") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits secs" as categorie,
            2023 as annee
        from {{ source("rnm", "fruits_secs_2023") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits secs" as categorie,
            2024 as annee
        from {{ source("rnm", "fruits_secs_2024") }}
        union all
        select
            stade,
            `Marché`,
            `Libellé`,
            `Unité`,
            cast(`Janvier` as int64) as `Janvier`,
            cast(`Février` as int64) as `Fevrier`,
            cast(`Mars` as int64),
            cast(`Avril` as int64),
            cast(`Mai` as int64),
            cast(`Juin` as int64),
            cast(`Juillet` as int64),
            cast(`Août` as int64),
            cast(`Septembre` as int64),
            cast(`Octobre` as int64),
            cast(`Novembre` as int64),
            cast(`Décembre` as int64),
            "Fruits secs" as categorie,
            2025 as annee
        from {{ source("rnm", "fruits_secs_2025") }}
    ),
    extract_cotation_by_month as (
        -- Janvier
        select
            categorie,
            marche,
            libelle,
            unite,
            janvier as cotation,
            date(annee, 01, 01) as date_cotation
        from all_csv
        union all
        -- Février
        select
            categorie,
            marche,
            libelle,
            unite,
            fevrier as cotation,
            date(annee, 02, 01) as date_cotation
        from all_csv
        union all
        -- Mars
        select
            categorie,
            marche,
            libelle,
            unite,
            mars as cotation,
            date(annee, 03, 01) as date_cotation
        from all_csv
        union all
        -- Avril
        select
            categorie,
            marche,
            libelle,
            unite,
            avril as cotation,
            date(annee, 04, 01) as date_cotation
        from all_csv
        union all
        -- Mai
        select
            categorie,
            marche,
            libelle,
            unite,
            mai as cotation,
            date(annee, 05, 01) as date_cotation
        from all_csv
        union all
        -- Juin
        select
            categorie,
            marche,
            libelle,
            unite,
            juin as cotation,
            date(annee, 06, 01) as date_cotation
        from all_csv
        union all
        -- Juillet
        select
            categorie,
            marche,
            libelle,
            unite,
            juillet as cotation,
            date(annee, 07, 01) as date_cotation
        from all_csv
        union all
        -- Aout
        select
            categorie,
            marche,
            libelle,
            unite,
            aout as cotation,
            date(annee, 08, 01) as date_cotation
        from all_csv
        union all
        -- Septembre
        select
            categorie,
            marche,
            libelle,
            unite,
            septembre as cotation,
            date(annee, 09, 01) as date_cotation
        from all_csv
        union all
        -- Octobre
        select
            categorie,
            marche,
            libelle,
            unite,
            octobre as cotation,
            date(annee, 10, 01) as date_cotation
        from all_csv
        union all
        -- Novembre
        select
            categorie,
            marche,
            libelle,
            unite,
            novembre as cotation,
            date(annee, 11, 01) as date_cotation
        from all_csv
        union all
        -- Décembre
        select
            categorie,
            marche,
            libelle,
            unite,
            decembre as cotation,
            date(annee, 12, 01) as date_cotation
        from all_csv
    ),

    clean_data as (
        select
            regexp_replace(
                regexp_extract(libelle, r'^([A-ZÉÈÀÙÂÊÎÔÛÇ]+(?:\s+[A-ZÉÈÀÙÂÊÎÔÛÇ]+)?)'),
                r'[\. ][A-ZÉÈÀÙÂÊÎÔÛÇ]{1}$',
                ""
            ) as produit,
            libelle,
            categorie,
            marche,
            unite,
            case
                when cotation > 99
                then cotation / 100
                when cotation > 9
                then cotation / 10
                else cotation
            end as prix_cotation,
            date_cotation,
            extract(year from date_cotation) as annee
        from extract_cotation_by_month
        where libelle is not null and unite like 'euro HT le kg'
    ),
    cotation_mois_precedent as (
        select
            *,
            lag(prix_cotation) over (
                partition by categorie, produit, libelle order by date_cotation
            ) as prix_mois_precedent,
        from clean_data
    ),
    variation_mois_precedent as (
        select
            *,
            if(
                prix_mois_precedent is not null,
                round(prix_cotation - prix_mois_precedent, 2),
                null
            ) as variation_mois_precedent,
            if(
                prix_mois_precedent is not null,
                round(
                    (prix_cotation - prix_mois_precedent) / prix_mois_precedent * 100, 2
                ),
                null
            ) as variation_pourcentage_prix_mois_precedent
        from cotation_mois_precedent
    ),
    source_data as (
        select *
        from variation_mois_precedent
        order by categorie, produit, libelle, date_cotation
    )

select *
from source_data

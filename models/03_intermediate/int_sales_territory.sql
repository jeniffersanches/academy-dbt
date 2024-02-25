{{ config(materialized= 'table') }}

with
    territorio as(
        select
            -- ids
            id_territorio
            -- nomes
            , nm_pais
            , nm_continente
            -- códigos
            , cd_pais
            -- valores
            , vr_venda_ytd
            , vr_venda_ano_passado
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_territory')}} 
    )

    , final_cte_territorio as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_territorio', 'nm_pais', 'nm_continente']) }} as sk_territorio
            , *
        from territorio              
    )

select *
from final_cte_territorio

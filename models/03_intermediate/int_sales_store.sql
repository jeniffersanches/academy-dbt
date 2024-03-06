{{ config(materialized= 'table') }}

with
    loja as (
        select
            -- ids
            id_entidade_negocio
            , nm_loja
            , id_vendedor
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_store')}}
    )

    , final_cte_loja as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_entidade_negocio', 'nm_loja', 'id_vendedor']) }} as sk_loja
            , *
        from loja             
    )

select *
from final_cte_loja

{{ config(materialized= 'table') }}

with 
    cartao_credito as (
        select 
            id_cartao_credito
            , nm_cartao_credito
            , num_cartao
            , mes_expiracao
            , ano_expiracao
            , dt_modificacao
        from {{ref('stg_sales_creditcard')}}
    )

    , final_cte_cartao_credito as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cartao_credito', 'nm_cartao_credito']) }} as sk_cartaocredito
            , *
        from cartao_credito
    )

select *
from final_cte_cartao_credito

{{ config(materialized= 'table') }}

with 
    cartao_credito as (
        select 
            id_cartao_credito
            , nm_cartao_credito
        from {{ref('stg_sales_creditcard')}}
    )

    , pessoa_cartao_credito as(
        select
            -- ids
            id_cartao_credito
            , id_entidade_negocio
            -- datas
            , dt_modificacao
        from{{ref('stg_sales_personcreditcard')}}
    )

    , dados_cartao as (
        select 
            cartao_credito.id_cartao_credito
            , pessoa_cartao_credito.id_entidade_negocio
            , COALESCE(cartao_credito.nm_cartao_credito, 'Não informado') as nm_cartao_credito
        from cartao_credito
        left join pessoa_cartao_credito
            on cartao_credito.id_cartao_credito = pessoa_cartao_credito.id_cartao_credito
    )

    , final_cte_cartao_credito as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cartao_credito', 'nm_cartao_credito']) }} as sk_cartaocredito
            , *
        from dados_cartao
    )

select *
from final_cte_cartao_credito

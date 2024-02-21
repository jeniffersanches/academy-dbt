{{ config(materialized= 'table') }}

with
    cabecalho_motivos_venda as(
        select
            -- ids
            id_pedido
            , id_motivo_venda
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_orderheadersalesreason')}}
    )

    , motivos_venda as(
        select
            -- ids
            id_motivo_venda
            -- nome e categoria/tipo motivo
            , nm_motivo
            , tp_motivo
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_reason')}}
    )

    , dados_motivo as (
        select
            cabecalho_motivos_venda.id_pedido
            , cabecalho_motivos_venda.id_motivo_venda
            , motivos_venda.nm_motivo
            , motivos_venda.tp_motivo
        from cabecalho_motivos_venda
        left join motivos_venda on cabecalho_motivos_venda.id_motivo_venda = motivos_venda.id_motivo_venda
    )

    , final_cte_motivo as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_pedido', 'id_motivo_venda']) }} as sk_motivo
            , *
        from dados_motivo
    )

select *
from final_cte_motivo
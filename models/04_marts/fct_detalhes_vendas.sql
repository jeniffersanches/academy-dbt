{{ config(materialized= 'table') }}

with
    final_cte_cartao_credito as (
        select
            *
        from {{ref('dim_cartaocredito')}}
    )

    , final_cte_cliente as (
        select
            *
        from {{ref('dim_cliente')}}
    )

    , final_cte_data as (
        select
            *
        from {{ref('dim_data')}}
    )

    , final_cte_motivo as (
        select
            *
        from {{ref('dim_motivo')}}
    )

    , final_cte_produto as (
        select
            *
        from {{ref('dim_produto')}}
    )

    , final_cte_regiao as (
        select
            *
        from {{ref('dim_regiao')}}
    )

    , final_cte_vendedor as (
        select
            *
        from {{ref('dim_vendedor')}}
    )

    , pedidos_com_sk as (
        select
            -- ids e chaves
            dime_motivo.sk_motivo as motivo_fk
            , dime_cliente.sk_cliente as cliente_fk
            , dime_vendedor.sk_vendedor as vendedor_fk
            , pedidos.id_territorio
            , dime_regiao.sk_regiao as regiao_fk
            , pedidos.id_endereco_entrega
            , dime_credito.sk_cartaocredito as cartao_credito_fk
            , dime_data.sk_data as data_fk
            -- datas
            , pedidos.dt_pedido
            , pedidos.dt_devida_entrega
            , pedidos.dt_envio
            , pedidos.dt_modificacao
            -- flags e outros
            , pedidos.st_pedido
            , pedidos.fl_pedido_online
            -- valores
            , pedidos.vr_subtotal
            , pedidos.vr_imposto
            , pedidos.vr_frete
            , pedidos.vr_total_devido
            -- origem
            , pedidos.nm_origem
        from {{ ref('stg_sales_orderheader')}} as pedidos
        left join final_cte_cartao_credito as dime_credito 
            on pedidos.id_cartao_credito = dime_credito.id_cartao_credito
        left join final_cte_cliente as dime_cliente
            on pedidos.id_cliente = dime_cliente.id_cliente
        left join final_cte_data as dime_data
            on pedidos.dt_pedido = dime_data.date_day
        left join final_cte_motivo as dime_motivo
            on pedidos.id_pedido = dime_motivo.id_pedido
        left join final_cte_regiao as dime_regiao
            on pedidos.id_endereco_cobranca = dime_regiao.id_endereco
        left join final_cte_vendedor as dime_vendedor
            on pedidos.id_vendedor = dime_vendedor.id_entidade_negocio
    )

select *
from pedidos_com_sk

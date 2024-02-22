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
            -- , pedidos.id_territorio
            , pedidos.id_pedido
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
            , case 
                when pedidos.st_pedido = 1 then 'Em processo'
                when pedidos.st_pedido = 2 then 'Aprovado'
                when pedidos.st_pedido = 3 then 'Pedido em espera'
                when pedidos.st_pedido = 4 then 'Rejeitado'
                when pedidos.st_pedido = 5 then 'Enviado'
                when pedidos.st_pedido = 6 then 'Cancelado'
            end as status_pedido
            , pedidos.fl_pedido_online
            -- valores
            , pedidos.vr_subtotal_pedido
            , pedidos.vr_imposto_pedido
            , pedidos.vr_frete_pedido
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

    , detalhes_pedidos_com_sk as (
        select
            -- ids
            detalhes_pedidos.id_pedido
            , dime_produto.sk_produto as produto_fk
            -- , detalhes_pedidos.id_detalhes_pedido
            -- quantidades
            , detalhes_pedidos.qt_encomendada_produto
            -- valores
            , detalhes_pedidos.vr_produto
            , detalhes_pedidos.vr_desconto  
            , detalhes_pedidos.total_linha_produto   
        from {{ref('stg_sales_orderdetail')}} as detalhes_pedidos   
        left join final_cte_produto as dime_produto
            on detalhes_pedidos.id_produto = dime_produto.id_produto 
    )

    , final as (
        select
            -- ids e fks
            pedidos_com_sk.motivo_fk
            , pedidos_com_sk.cliente_fk
            , pedidos_com_sk.vendedor_fk
            , pedidos_com_sk.regiao_fk
            , pedidos_com_sk.cartao_credito_fk
            , pedidos_com_sk.data_fk
            , detalhes_pedidos_com_sk.produto_fk
            , detalhes_pedidos_com_sk.id_pedido
            -- datas
            , pedidos_com_sk.dt_pedido
            , pedidos_com_sk.dt_devida_entrega
            , pedidos_com_sk.dt_envio
            , pedidos_com_sk.dt_modificacao
            -- flags e status
            , pedidos_com_sk.status_pedido
            , pedidos_com_sk.fl_pedido_online
            -- valores
            , pedidos_com_sk.vr_subtotal_pedido
            , pedidos_com_sk.vr_imposto_pedido
            , pedidos_com_sk.vr_frete_pedido
            , pedidos_com_sk.vr_total_devido 
            , detalhes_pedidos_com_sk.vr_produto  
            , detalhes_pedidos_com_sk.vr_desconto 
            -- quantidades
            , detalhes_pedidos_com_sk.qt_encomendada_produto
            , detalhes_pedidos_com_sk.total_linha_produto 
            -- , total_produto.total_linha_produto
        from pedidos_com_sk     
        left join detalhes_pedidos_com_sk
            on pedidos_com_sk.id_pedido = detalhes_pedidos_com_sk.id_pedido   
    )

select *
from final

with
    cabecalho_motivos_venda as(
        select
            -- ids
            salesorderid as id_pedido
            , salesreasonid as id_motivo_venda
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','salesorderheadersalesreason')}}
    )

select *
from cabecalho_motivos_venda

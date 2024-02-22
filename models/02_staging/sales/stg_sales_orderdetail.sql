with
    detalhes_venda as (
        select
            -- ids
            salesorderid as id_pedido
            , productid as id_produto
            , salesorderdetailid as id_detalhes_pedido
            -- quantidades
            , orderqty as qt_encomendada_produto
            -- valores
            , ROUND(unitprice, 2) as vr_produto
            , ROUND(unitpricediscount, 2) as vr_desconto
            , ROUND(COALESCE((unitprice * (1.0 - COALESCE(unitpricediscount, 0.0)) * orderqty), 0.0), 2) as total_linha_produto
            -- origem
            , 'sales' as nm_origem
            -- datas
            , date(modifieddate) as dt_modificacao
        from {{ source('sales','salesorderdetail')}}
    )

select *
from detalhes_venda

with
    validacao_quantidade_pedidos as (
        select 
            sum(qt_encomendada_produto) as qt_produtos
        from {{ref('fct_vendas_item')}}
    )

select *
from validacao_quantidade_pedidos 
where qt_produtos != 274914

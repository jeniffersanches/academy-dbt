{{ config(materialized= 'table') }}

with 
    vendas_agg_regiao_vendedor as (
        select
            dim_regiao.nm_pais
            , dim_vendedor.id_entidade_negocio as id_vendedor
            , dim_vendedor.nm_completo as nm_vendedor
            , ROUND(SUM(fct_vendas_item.total_linha_produto), 2) as total_vendas
        from 
            {{ ref('fct_vendas_item') }} fct_vendas_item
        left join {{ ref('dim_regiao') }} dim_regiao 
            on dim_regiao.sk_regiao = fct_vendas_item.regiao_fk
        left join {{ ref('dim_vendedor') }} dim_vendedor
            on dim_vendedor.sk_vendedor = fct_vendas_item.vendedor_fk
        where   
            dim_vendedor.id_entidade_negocio is not null and dim_vendedor.nm_completo is not null
        group by
            dim_regiao.nm_pais
            , dim_vendedor.id_entidade_negocio
            , dim_vendedor.nm_completo
    )

select * 
from vendas_agg_regiao_vendedor

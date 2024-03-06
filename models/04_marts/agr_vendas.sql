{{ config(materialized= 'table') }}

with 
    vendas_agr_regiao_vendedor as (
        select
            territorio.sk_territorio as territorio_fk
            , dim_vendedor.sk_vendedor as vendedor_fk
            , dim_data.sk_data as data_fk
            , dim_data.month_of_year as mes
            , dim_data.nm_mes_abrv
            , dim_data.year_number as ano
            , territorio.nm_pais
            , dim_vendedor.nm_completo as nm_vendedor
            , case 
                when vendas.fl_pedido_online = true then 'Online'
                when vendas.fl_pedido_online = false then 'Físico'
            end as canal_venda
            , vendas.vr_total_devido 
        from 
            {{ ref('stg_sales_orderheader') }} vendas
        left join {{ ref('int_sales_territory') }} territorio
            on territorio.id_territorio = vendas.id_territorio
        left join {{ ref('dim_vendedor') }} dim_vendedor
            on dim_vendedor.id_entidade_negocio = vendas.id_vendedor
        left join {{ref('dim_data')}} dim_data
            on dim_data.date_day = vendas.dt_pedido
        group by
            territorio.sk_territorio
            , dim_vendedor.sk_vendedor
            , dim_data.sk_data
            , dim_data.month_of_year
            , dim_data.nm_mes_abrv
            , dim_data.year_number
            , territorio.nm_pais
            , dim_vendedor.nm_completo
            , vendas.fl_pedido_online
            , vendas.vr_total_devido
        order by
            dim_data.year_number
            , dim_data.month_of_year 
    )

select * 
from vendas_agr_regiao_vendedor

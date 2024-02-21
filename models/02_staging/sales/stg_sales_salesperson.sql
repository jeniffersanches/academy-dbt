with
    vendedor as (
        select
            -- ids
            businessentityid as id_entidade_negocio
            , territoryid as id_territorio
            -- valores
            , salesquota as projecao_vendas_anuais
            , bonus 
            , commissionpct as vr_comissao
            , salesytd as vr_venda_ytd
            , saleslastyear as vr_venda_ano_passado
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','salesperson')}}
    )

select *
from vendedor

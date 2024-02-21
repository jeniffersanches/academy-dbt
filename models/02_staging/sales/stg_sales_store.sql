with
    loja as (
        select
            -- ids
            businessentityid as id_entidade_negocio
            , name as nm_loja
            , salespersonid as id_vendedor
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','store')}}
    )

select *
from loja

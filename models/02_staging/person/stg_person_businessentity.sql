with
    entidade_negocio as (
        select
            -- ids
            businessentityid as id_entidade_negocio
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('person','businessentity')}}
    )

select *
from entidade_negocio

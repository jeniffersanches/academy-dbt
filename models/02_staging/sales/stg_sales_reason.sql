with
    motivos_venda as(
        select
            -- ids
            salesreasonid as id_motivo_venda
            -- nome e categoria/tipo motivo
            , name as nm_motivo
            , reasontype as tp_motivo
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','salesreason')}}
    )

select *
from motivos_venda

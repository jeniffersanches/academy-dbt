with
    provincia_estado as (
        select
            -- ids
            stateprovinceid as id_estado
            , territoryid as id_territorio
            -- códigos
            , stateprovincecode as cd_estado
            , countryregioncode as cd_pais
            -- nomes
            , name as nm_estado
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('person','stateprovince')}}
    )

select *
from provincia_estado

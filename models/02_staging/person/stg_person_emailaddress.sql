with
    endereco_email as (
        select
            -- ids
            businessentityid as id_entidade_negocio
            , emailaddressid as id_endereco_email
            -- outros
            , cast(emailaddress.emailaddress as string)  as email
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('person','emailaddress')}}
    )

select *
from endereco_email

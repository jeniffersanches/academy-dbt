with
    pessoa_cartao_credito as(
        select
            -- ids
            creditcardid as id_cartao_credito
            , businessentityid as id_entidade_negocio
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','personcreditcard')}}
    )

select *
from pessoa_cartao_credito

with
    cartao_credito as(
        select
            -- ids
            creditcardid as id_cartao_credito
            -- nomes
            , cardtype as nm_cartao_credito
            , cardnumber as num_cartao
            -- datas
            , expmonth as mes_expiracao
            , expyear as ano_expiracao
            , date(modifieddate) as dt_modificacao
        from{{ source('sales','creditcard')}}
    )

select *
from cartao_credito

{{ config(materialized= 'table') }}

with
    cliente as(
        select
            -- ids
            id_cliente
            , id_pessoa
            , id_loja
            , id_territorio
        from{{ref('stg_sales_customer')}}
    )

    , pessoa as (
        select
            -- ids
            id_entidade_negocio
            -- nomes
            , tp_pessoa
            , nm_pessoa
            , nm_meio
            , nm_ultimo
            , nm_completo
            -- outros
            , emailpromotion
        from{{ref('stg_person_person')}}
    )

    , endereco_email as (
        select
            -- ids
            id_entidade_negocio
            , id_endereco_email
            -- outros
            , email
        from{{ref('stg_person_emailaddress')}}
    )

    , dados_cliente as (
        select 
            cliente.id_cliente
            , pessoa.id_entidade_negocio
            , case
                when pessoa.tp_pessoa = 'SC' then 'Contato Loja'
                when pessoa.tp_pessoa = 'IN' then 'Pessoa Física'
                when pessoa.tp_pessoa = 'SP' then 'Vendedor'
                when pessoa.tp_pessoa = 'EM' then 'Funcionário (não vendas)'
                when pessoa.tp_pessoa = 'VC' then 'Contato do Fornecedor'
                when pessoa.tp_pessoa = 'GC' then 'Contato Geral'
            end as tipo_pessoa
            , pessoa.nm_pessoa
            , pessoa.nm_meio
            , pessoa.nm_ultimo
            , pessoa.nm_completo
            , pessoa.emailpromotion 
            , endereco_email.email
        from cliente
        left join pessoa on cliente.id_cliente = pessoa.id_entidade_negocio
        left join endereco_email on pessoa.id_entidade_negocio = endereco_email.id_entidade_negocio
    )

    , final_cte_cliente as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_cliente', 'id_entidade_negocio', 'nm_completo']) }} as sk_cliente
            , *
        from dados_cliente
    )

select *
from final_cte_cliente

{{ config(materialized= 'table') }}

with
    vendedor as (
        select
            -- ids
            id_entidade_negocio
            , id_territorio
            -- valores
            , projecao_vendas_anuais
            , bonus 
            , vr_comissao
            , vr_venda_ytd
            , vr_venda_ano_passado
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_salesperson')}}
    )

    , funcionarios as (
        select
            -- ids
            id_entidade_negocio
            , id_login_funcionario
            -- outros
            , cargo
        from{{ ref('stg_human_resources_employee')}}
    )

    , pessoa as(
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
            -- datas
            , dt_modificacao
        from{{ ref('stg_person_person')}}
    )

    , dados_vendedor as (
        select
            vendedor.id_entidade_negocio
            , vendedor.id_territorio
            , funcionarios.cargo
            , pessoa.nm_completo
        from vendedor
        left join funcionarios on vendedor.id_entidade_negocio = funcionarios.id_entidade_negocio
        left join pessoa on funcionarios.id_entidade_negocio = pessoa.id_entidade_negocio
        where pessoa.tp_pessoa = 'SP'
    )

    , final_cte_vendedor as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_entidade_negocio', 'id_territorio']) }} as sk_vendedor
            , *
        from dados_vendedor
    )

select *
from final_cte_vendedor

{{ config(materialized= 'table') }}

with
    endereco_pessoa as (
        select
            -- ids
            id_endereco
            , id_estado
            -- outros
            , endereco
            , cidade
            -- datas
            , dt_modificacao
        from{{ ref('stg_person_address')}}
    )

    , provincia_estado as (
        select
            -- ids
            id_estado
            , id_territorio
            -- códigos
            , cd_estado
            , cd_pais
            -- nomes
            , nm_estado
            -- datas
            , dt_modificacao
        from{{ ref('stg_person_stateprovince')}}
    )

    , pais_regioes as (
        select
            -- códigos
            cd_pais
            -- nomes
            , nm_pais
            -- datas
            , dt_modificacao
        from{{ ref('stg_person_countryregion')}}
    )

    , territorio as (
        select 
            -- ids
            id_territorio
            -- nomes
            , nm_pais
            , nm_continente
            -- códigos
            , cd_pais
            -- valores
            , vr_venda_ytd
            , vr_venda_ano_passado
            -- datas
            , dt_modificacao
        from{{ ref('stg_sales_territory')}} 
    )

    , dados_regiao as (
        select
            endereco_pessoa.id_endereco
            , endereco_pessoa.id_estado
            , endereco_pessoa.endereco
            , endereco_pessoa.cidade
            , provincia_estado.id_territorio
            , provincia_estado.cd_estado
            , provincia_estado.cd_pais
            , provincia_estado.nm_estado
            , pais_regioes.nm_pais
            , territorio.nm_continente
        from endereco_pessoa
        left join provincia_estado 
            on endereco_pessoa.id_estado = provincia_estado.id_estado
        left join pais_regioes 
            on provincia_estado.cd_pais = pais_regioes.cd_pais
        left join territorio 
            on provincia_estado.id_territorio = territorio.id_territorio
    )

    , final_cte_regiao as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_endereco', 'endereco', 'cidade']) }} as sk_regiao
            , *
        from dados_regiao
    )

select *
from final_cte_regiao

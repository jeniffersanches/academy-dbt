{{ config(materialized= 'table') }}

with

    produto as (
        select
            -- ids
            id_produto
            , id_subcategoria_produto
            -- nomes
            , nm_produto
            -- códigos
            , cd_produto
            -- flags
            , fl_tipo_producao
            -- outros
            , cor_produto
            , nivel_seguro_estoque
            , nivel_recompra_estoque
            , tamanho_produto
            , linha_produto
            , estilo_produto
            -- valores
            , vr_preco_padrao
            , vr_venda_produto
            -- datas
            , dt_modificacao
        from{{ ref('stg_production_product')}}
    )

    , subcategoria_produto as(
        select
            -- ids
            id_subcategoria_produto
            , id_categoria_produto
            -- nomes
            , nm_subcategoria_produto
            -- datas
            , dt_modificacao
        from{{ ref('stg_production_productsubcategory')}}
    )

    , categoria_produto as(
        select
            -- ids
            id_categoria_produto
            -- nomes
            , nm_categoria_produto
            -- datas
            , dt_modificacao
        from{{ ref('stg_production_productcategory')}}
    )

    , dados_produto as (
        select 
            produto.id_produto
            , produto.id_subcategoria_produto
            , produto.nm_produto
            , produto.cd_produto
            , produto.cor_produto
            , produto.estilo_produto
            , subcategoria_produto.id_categoria_produto
            , subcategoria_produto.nm_subcategoria_produto
            , categoria_produto.nm_categoria_produto
        from produto
        left join subcategoria_produto on produto.id_subcategoria_produto = subcategoria_produto.id_subcategoria_produto
        left join categoria_produto on subcategoria_produto.id_categoria_produto = categoria_produto.id_categoria_produto
    )

    , final_cte_produto as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_produto', 'cd_produto']) }} as sk_produto
            , *
        from dados_produto     
    )

select *
from final_cte_produto

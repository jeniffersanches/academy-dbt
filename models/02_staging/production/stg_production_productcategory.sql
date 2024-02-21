with
    categoria_produto as(
        select
            -- ids
            productcategoryid as id_categoria_produto
            -- nomes
            , name as nm_categoria_produto
            -- datas
            , date(modifieddate) as dt_modificacao
        from{{ source('production','productcategory')}}
    )

select *
from categoria_produto

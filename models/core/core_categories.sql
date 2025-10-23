{{ config(
    materialized='table',
) }}

with categories as (
    select * from (values
        (1::smallint, 'Advertising'),
        (2::smallint, 'Politics & Society'),
        (3::smallint, 'Economy, Work & Money'),
        (4::smallint, 'Health, Body & Mind'),
        (5::smallint, 'Relationships & Social Life'),
        (6::smallint, 'Science, Technology & Digital Life'),
        (7::smallint, 'Culture, Arts & Media'),
        (8::smallint, 'Environment & Planet'),
        (9::smallint, 'History, Identity & Heritage'),
        (10::smallint, 'Dark, Edgy & Absurd Humor')
    ) as t(category_id, main_category)
)

select
    category_id,
    main_category
from categories

{{ config(
    materialized='table',
) }}

with subcategories as (
    select * from (values
        (1::smallint, 1::smallint, 'Upcoming shows & live events'),
        (2::smallint, 1::smallint, 'Streaming platforms & online services'),
        (3::smallint, 1::smallint, 'Marketplaces & e-commerce'),
        (4::smallint, 1::smallint, 'Tech products & gadgets'),
        (5::smallint, 1::smallint, 'Food & beverages'),
        (6::smallint, 1::smallint, 'Travel & lifestyle services'),
        (7::smallint, 1::smallint, 'Finance & banking apps'),
        (8::smallint, 1::smallint, 'Health & fitness products'),
        (9::smallint, 1::smallint, 'Mobile operators & internet providers'),
        (10::smallint, 1::smallint, 'Education & online courses'),
        (11::smallint, 2::smallint, 'Political satire'),
        (12::smallint, 2::smallint, 'Social commentary & inequality'),
        (13::smallint, 2::smallint, 'Human rights & activism'),
        (14::smallint, 2::smallint, 'Immigration & migration'),
        (15::smallint, 2::smallint, 'Law, crime & justice'),
        (16::smallint, 2::smallint, 'Censorship & freedom of speech'),
        (17::smallint, 3::smallint, 'Workplace humor & office culture'),
        (18::smallint, 3::smallint, 'Money & personal finance'),
        (19::smallint, 3::smallint, 'Career struggles & unemployment'),
        (20::smallint, 3::smallint, 'Business & entrepreneurship'),
        (21::smallint, 4::smallint, 'Physical health & fitness'),
        (22::smallint, 4::smallint, 'Mental health & therapy'),
        (23::smallint, 4::smallint, 'Addictions & substance use'),
        (24::smallint, 4::smallint, 'Aging & body image'),
        (25::smallint, 5::smallint, 'Dating & romance'),
        (26::smallint, 5::smallint, 'Marriage & family life'),
        (27::smallint, 5::smallint, 'Friendship & social circles'),
        (28::smallint, 5::smallint, 'Sex & intimacy'),
        (29::smallint, 5::smallint, 'Parenting'),
        (30::smallint, 6::smallint, 'Tech & gadgets'),
        (31::smallint, 6::smallint, 'Internet culture, memes & influencers'),
        (32::smallint, 6::smallint, 'Artificial intelligence & future tech'),
        (33::smallint, 6::smallint, 'Science news & discoveries'),
        (34::smallint, 7::smallint, 'Movies, TV & streaming'),
        (35::smallint, 7::smallint, 'Music & live performance'),
        (36::smallint, 7::smallint, 'Literature & art references'),
        (37::smallint, 7::smallint, 'Celebrities & fame'),
        (38::smallint, 7::smallint, 'Pop culture trends'),
        (39::smallint, 8::smallint, 'Climate change & sustainability jokes'),
        (40::smallint, 8::smallint, 'Animals & pets'),
        (41::smallint, 8::smallint, 'Urban vs rural life'),
        (42::smallint, 8::smallint, 'Natural disasters & weather humor'),
        (43::smallint, 9::smallint, 'Historical events satire'),
        (44::smallint, 9::smallint, 'Cultural traditions & heritage'),
        (45::smallint, 9::smallint, 'Generational differences & nostalgia'),
        (46::smallint, 9::smallint, 'National stereotypes'),
        (47::smallint, 10::smallint, 'Morbid comedy & death jokes'),
        (48::smallint, 10::smallint, 'Offensive or taboo topics'),
        (49::smallint, 10::smallint, 'Self-deprecating humor'),
        (50::smallint, 10::smallint, 'Surreal or absurd comedy')
    ) as t(subcategory_id, category_id, subcategory)
)

select
    subcategory_id,
    category_id,
    subcategory
from subcategories

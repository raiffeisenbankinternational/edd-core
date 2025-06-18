----
-- glms-dimension-svc, 1.5M on prod
----

insert into test_glms_dimension_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'state', ':active',
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'entity-category', ':standalone-customer',
            'industry-matrix-quadrant', format('L%s', x),
            'gics-level-3-long-name', format('Generated Name %s', x),
            'top-parent-id', format('#%s', gen_random_uuid()),
            'gics-level-1-code', format('%s', x),
            'pam-first-name', format('pam-first-name-%s', x % 100),
            'short-name', format('short-name-%s', x),
            'entity-type', ':corporate',
            'top-parent-short-name', format('Parent Name %s', x),
            'legal-form', '',
            'public-flag', 'N',
            'nace-code', format('123%s', x),
            'oenb-ident-number', format('OENB-%s', x),
            'gam-unit', jsonb_build_array(
                'RBI',
                'RBRU'
            ),
            'cam-first-name', format('cam-first-name-%s', x % 100),
            'pam-org-unit-name', '',
            'gcc-area', jsonb_build_array(
                ':workout'
            ),
            'cam-last-name', format('cam-last-name-%s', x % 100),
            'active-flag', 'Y',
            'local-customer', jsonb_build_array(
                jsonb_build_object(
                    'unit-code', 'RBRU',
                    'am-last-name', format('LastName-%s', x),
                    'am-first-name', format('FirstName-%s', x),
                    'local-customer-number', format('A%sB%sC', x, x),
                    'am-code', format('CODE-%s', x)
                )
            ),
            'cam-code', format('CAM%s', x),
            'city', format('Chita-%s', x),
            'cam-org-unit-name', format('cam-org-unit-name-%s', x),
            'gics-level-3-code', format('%s', x),
            'country-of-risk', format('XX-%s', x % 100),
            'state', format('Some Country %s', x),
            'pam-unit', format('PAM-%s', x),
            'street', format('Street Foo-%s', x),
            'pam-last-name', format('PAM last name-%s', x % 100),
            'gics-level-1-short-name', format('ABC-%s', x % 100),
            'asset-class', jsonb_build_object(
                'asset-class-name', format('Asset Class %s', x % 100),
                'asset-class-code', x % 100
            ),
            'nace-name', format('Some Name %s', x),
            'segment', 'RET',
            'main-unit-code', 'RBRU',
            'gics-level-1-long-name', format('Foo Bar %s', x),
            'cam-unit', format('CAM-%s', x),
            'org-level', format('Lvl-%s', x),
            'cocunut', format('123456%s', x),
            'gams-client-code', 'N',
            'portfolio', 'RET',
            'person-flag', 'N',
            'pam-code', format('PAM-%s', x),
            'gics-level-3-short-name', format('ABC-%s', x % 100),
            'zip-code', format('223-%s', x),
            'gics-level-3-name', format('Foo Bar - L3 - %s', x),
            'gics-level-1-name', format('Foo Test - ABC - %s', x),
            'country-of-domicile', 'RU',
            'top-parent-cocunut', format('987654321%s', x),
            'rating', jsonb_build_object(
                'rating-note-country', format('%s', x)
            )
        ),
        'version', 2
    ) as aggregate
from
    generate_series(1, 1500000) as gen(x)
returning id;

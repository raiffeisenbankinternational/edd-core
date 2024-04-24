----
-- glms-exposure-svc, 2M on prod
----

insert into test_glms_exposure_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'type', ':exposure-facility',
        'state', ':active',
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'amount', jsonb_build_object(
                'EUR', x * 10
            ),
            'match-candidate-attr5', format('FOOBAR%s#ABC', x),
            'risk-taker-id', format('#%s', gen_random_uuid()),
            'gpc-code', format('FL%s', x),
            'product-set-id', format('#%s', gen_random_uuid()),
            'exclude-from-netting', 'T',
            'end-date', '2025-06-21',
            'parent-facility', format('%s@facility.ABC', x),
            'risk-taker', format('ABC%s', x),
            'contract-amount', 10 * x,
            'parent-facility-id', format('#%s', gen_random_uuid()),
            'ufn', 'N',
            'banking-book', 'BANKING',
            'revolving', 'Y',
            'match-candidate-link', format('%s@facility.ABC', x),
            'parent-facility-source', ':match-candidate-edwh',
            'match-candidate-edwh', format('%s@facility.ABC', x),
            'risk-on-id', format('#%s', gen_random_uuid()),
            'matching-type', ':migration',
            'attribute-5', format('FOOBAR%s#AAA*BBB*CCC*DDD*XYZ', x),
            'on-off-balance', 'OFF',
            'risk-on', format('123%s', x),
            'cancelable-flag', 'N',
            'drawing-type', ':commercial',
            'business-date', '2024-03-28',
            'source-system', format('ABC%s', x),
            'contract-reference', format('123123123%s', x),
            'tenor-type', ':rolling',
            'start-date', '2023-06-21',
            'booking-system', 'ABC',
            'disbursement-type', 'DUNNO',
            'commitment-type', 'COMMITTED'
        ),
        'version', 13
    ) as aggregate
from
    generate_series(1, 2000000) as gen(x)
returning id;

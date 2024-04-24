----
-- glms-facility-svc, 300K on prod
----

insert into test_glms_facility_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'credit-risk-mitigation', jsonb_build_object(
                'links', jsonb_build_array(
                    jsonb_build_object(
                        'wv-mitigation-allocation', jsonb_build_object(
                            'allocated-amount', jsonb_build_object(
                                'EUR', x
                            ),
                            'mitigation-allocation-type', ':amount'
                        ),
                        'provider-id', format('#%s', gen_random_uuid()),
                        'crm-id', format('#%s', gen_random_uuid()),
                        'perfected', ':yes',
                        'contract', jsonb_build_object(
                            'category', 'guarantee',
                            'object-codes', jsonb_build_array(
                                format('Code%s', x)
                            ),
                            'gct-id', format('Code%s', x)
                        ),
                        'nominal-amount', x,
                        'nv-mitigation-allocation', jsonb_build_object(
                            'allocated-amount', jsonb_build_object(
                                'EUR', x
                            ),
                            'mitigation-allocation-type', ':amount'
                        ),
                        'facility-external-id', format('123%s', x),
                        'cdc-change-cd', 'U',
                        'weighted-crm-value', x,
                        'crm-external-code', format('312%s', x),
                        'provider', jsonb_build_object(
                            'short-name', format('Some Name %s', x),
                            'country-of-risk', 'BA',
                            'gcc-name', format('Some Group %s', x)
                        )
                    ),
                    jsonb_build_object(
                        'wv-mitigation-allocation', jsonb_build_object(
                            'allocated-amount', jsonb_build_object(
                                'EUR', x + 123
                            ),
                            'mitigation-allocation-type', ':amount'
                        ),
                        'provider-id', format('#%s', gen_random_uuid()),
                        'crm-id', format('#%s', gen_random_uuid()),
                        'perfected', ':yes',
                        'contract', jsonb_build_object(
                            'category', 'insurance',
                            'object-codes', jsonb_build_array(
                                format('Code%s', x)
                            ),
                            'gct-id', format('Code%s', x)
                        ),
                        'facility-external-id', format('184%s', x),
                        'cdc-change-cd', 'U',
                        'weighted-crm-value', 0,
                        'crm-external-code', format('184%s', x),
                        'provider', jsonb_build_object(
                            'short-name', format('Some Name %s', x),
                            'country-of-risk', 'BA',
                            'gcc-name', format('Some Group %s', x)
                        )
                    ),
                    jsonb_build_object(
                        'wv-mitigation-allocation', jsonb_build_object(
                            'allocated-amount', jsonb_build_object(
                                'EUR', 135 + x
                            ),
                            'mitigation-allocation-type', ':amount'
                        ),
                        'provider-id', format('#%s', gen_random_uuid()),
                        'crm-id', format('#%s', gen_random_uuid()),
                        'perfected', ':yes',
                        'contract', jsonb_build_object(
                            'category', format('foo%s', x),
                            'object-codes', jsonb_build_array(
                                format('CAR%s', x)
                            ),
                            'gct-id', format('FOO%s', x)
                        ),
                        'facility-external-id', format('99%s', x),
                        'cdc-change-cd', 'U',
                        'weighted-crm-value', 135 + x,
                        'crm-external-code', format('990%s', x),
                        'provider', jsonb_build_object(
                            'short-name', format('Some Name %s', x),
                            'country-of-risk', 'BA',
                            'gcc-name', format('Some Group %s', x)
                        )
                    )
                )
            )
        ),
        'version', 1
    ) as aggregate
from
    generate_series(1, 500000) as gen(x)
returning id;

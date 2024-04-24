----
-- glms-application-svc, 13K on prod
----

insert into test_glms_application_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'application-points-in-time', jsonb_build_array(
            jsonb_build_object(
                'transition', jsonb_build_object(
                    'from', ':created',
                    'to', ':risk-assessment'
                ),
                'point-in-time-id', format('#%s', gen_random_uuid()),
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'transition', jsonb_build_object(
                    'from', ':risk-assessment',
                    'to', ':waiting-decision'
                ),
                'point-in-time-id', format('#%s', gen_random_uuid()),
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'transition', jsonb_build_object(
                    'from', ':waiting-decision',
                    'to', ':verification'
                ),
                'point-in-time-id', null,
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'transition', jsonb_build_object(
                    'from', ':verification',
                    'to', ':approved'
                ),
                'point-in-time-id', format('#%s', gen_random_uuid()),
                'as-of-date', '2024-04-10'
            )
        ),
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'credit-risk-policy', ':met',
            'application-id', x,
            'assignees', jsonb_build_object(
                'roles', jsonb_build_array(
                    ':lime-account-managers',
                    ':lime-risk-managers',
                    ':lime-verifiers'
                ),
                'departments', jsonb_build_array(
                    jsonb_build_object(
                        'department', format('Department - %s', x),
                        'department-code', format('DEP%s', x)
                    )
                ),
                'users', jsonb_build_array(
                    jsonb_build_object(
                        'role', ':lime-verifiers',
                        'status', ':active',
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Department - %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                )
            ),
            'credit-policy-relevance', ':yes',
            'process-flow', ':standard',
            'decision-authority-parameters', null,
            'request-section-id', format('#%s', gen_random_uuid()),
            'authority', jsonb_build_object(
                'committee', ':credit-committee'
            ),
            'requests', jsonb_build_array(
                jsonb_build_object(
                    'request-type', ':limitation-review-request',
                    'id', format('#%s', gen_random_uuid())
                ),
                jsonb_build_object(
                    'request-type', ':tcmg-facility',
                    'customer', jsonb_build_object(
                        'short-name', format('FooBar-%s', x),
                        'id', format('#%s', gen_random_uuid()),
                        'cocunut', format('%s', x)
                    ),
                    'id', format('#%s', gen_random_uuid())
                ),
                jsonb_build_object(
                    'request-type', ':tcmg-facility',
                    'customer', jsonb_build_object(
                        'short-name', format('Hello World %s', x),
                        'id', format('#%s', gen_random_uuid()),
                        'cocunut', format('123%s', x)
                    ),
                    'id', format('#%s', gen_random_uuid())
                )
            ),
            'decision-type', ':approval',
            'credit-policy-compliance-v2', jsonb_build_object(
                'credit-policy-details', jsonb_build_array(
                    jsonb_build_object(
                        'risk-on-cocunut', format('9876%s', x),
                        'credit-policy-cap', jsonb_build_object(
                            'EUR', 100 + x
                        ),
                        'type', ':calculated',
                        'approved', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 200 + x
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', 300 + x
                            )
                        ),
                        'consolidation-point', jsonb_build_object(
                            'risk-taker-id', format('#%s', gen_random_uuid()),
                            'product-set-id', format('#%s', gen_random_uuid()),
                            'risk-on-id', format('#%s', gen_random_uuid())
                        ),
                        'risk-on-short-name', format('Some Name %s', x),
                        'change', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 14700000000
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', -3400000000
                            )
                        ),
                        'requested', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 14700000000
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', 15300000000
                            )
                        ),
                        'risk-on-cpar', '3A',
                        'compliance', ':met'
                    ),
                    jsonb_build_object(
                        'risk-on-cocunut', format('%s', x),
                        'credit-policy-cap', jsonb_build_object(
                            'EUR', 30000000000
                        ),
                        'type', ':calculated',
                        'approved', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 25450000000
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', 4550000000
                            )
                        ),
                        'consolidation-point', jsonb_build_object(
                            'risk-taker-id', format('#%s', gen_random_uuid()),
                            'product-set-id', format('#%s', gen_random_uuid()),
                            'risk-on-id', format('#%s', gen_random_uuid())
                        ),
                        'risk-on-short-name', format('ShortName%s', x),
                        'change', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 123 + x
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', 111 + x
                            )
                        ),
                        'requested', jsonb_build_object(
                            'hdr', jsonb_build_object(
                                'EUR', 555 + x
                            ),
                            'risk-limit', jsonb_build_object(
                                'EUR', 999 + x
                            )
                        ),
                        'risk-on-cpar', '3A',
                        'compliance', ':met'
                    )
                ),
                'compliance', ':met'
            ),
            'journal', jsonb_build_array(
                jsonb_build_object(
                    'event-id', ':application-created',
                    'time', '2024-04-10T05:47:59.498Z',
                    'status', ':created',
                    'user', jsonb_build_object(
                        'role', ':lime-account-managers',
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Some Department %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                ),
                jsonb_build_object(
                    'event-id', ':sent-to-risk-assessment',
                    'time', '2024-04-10T05:51:19.786Z',
                    'status', ':risk-assessment',
                    'user', jsonb_build_object(
                        'role', ':lime-account-managers',
                        'email', format('user-%s@test.com', x),
                        'decisionlevel', jsonb_build_array(
                            ':professionalmanager'
                        ),
                        'roles', jsonb_build_array(
                            ':lime-readers',
                            ':lime-workout-managers',
                            ':lime-product-managers',
                            ':lime-limit-managers',
                            ':lime-risk-managers',
                            ':lime-account-managers',
                            ':lime-tcm-releaser',
                            ':lime-verifiers'
                        ),
                        'function', jsonb_build_array(
                            ':business'
                        ),
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Some Department %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'action', jsonb_build_array(
                            ':switch-realm'
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                ),
                jsonb_build_object(
                    'event-id', ':sent-for-decision',
                    'time', '2024-04-10T05:53:07.582Z',
                    'status', ':waiting-decision',
                    'user', jsonb_build_object(
                        'role', ':lime-risk-managers',
                        'email', format('user-%s@test.com', x),
                        'decisionlevel', jsonb_build_array(
                            ':professionalmanager'
                        ),
                        'roles', jsonb_build_array(
                            ':lime-readers',
                            ':lime-workout-managers',
                            ':lime-product-managers',
                            ':lime-limit-managers',
                            ':lime-risk-managers',
                            ':lime-account-managers',
                            ':lime-tcm-releaser',
                            ':lime-verifiers'
                        ),
                        'function', jsonb_build_array(
                            ':business'
                        ),
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Some Department %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'action', jsonb_build_array(
                            ':switch-realm'
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                ),
                jsonb_build_object(
                    'event-id', ':application-in-verification',
                    'time', '2024-04-10T05:53:39.168Z',
                    'status', ':verification',
                    'user', jsonb_build_object(
                        'role', ':lime-verifiers',
                        'email', format('user-%s@test.com', x),
                        'decisionlevel', jsonb_build_array(
                            ':professionalmanager'
                        ),
                        'roles', jsonb_build_array(
                            ':lime-readers',
                            ':lime-workout-managers',
                            ':lime-product-managers',
                            ':lime-limit-managers',
                            ':lime-risk-managers',
                            ':lime-account-managers',
                            ':lime-tcm-releaser',
                            ':lime-verifiers'
                        ),
                        'function', jsonb_build_array(
                            ':business'
                        ),
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Some Department %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'action', jsonb_build_array(
                            ':switch-realm'
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                ),
                jsonb_build_object(
                    'event-id', ':application-approved',
                    'time', '2024-04-10T05:54:36.048Z',
                    'status', ':approved',
                    'user', jsonb_build_object(
                        'role', ':lime-verifiers',
                        'email', format('user-%s@test.com', x),
                        'decisionlevel', jsonb_build_array(
                            ':professionalmanager'
                        ),
                        'roles', jsonb_build_array(
                            ':lime-readers',
                            ':lime-workout-managers',
                            ':lime-product-managers',
                            ':lime-limit-managers',
                            ':lime-risk-managers',
                            ':lime-account-managers',
                            ':lime-tcm-releaser',
                            ':lime-verifiers'
                        ),
                        'function', jsonb_build_array(
                            ':business'
                        ),
                        'id', format('user-%s@test.com', x),
                        'attrs', jsonb_build_object(
                            'department', format('Some Department %s', x),
                            'department-code', format('DEP%s', x)
                        ),
                        'action', jsonb_build_array(
                            ':switch-realm'
                        ),
                        'units', jsonb_build_array(
                            ':rbspk',
                            ':rbhr',
                            ':rbi',
                            ':rbby',
                            ':aval',
                            ':rbbh',
                            ':rbro',
                            ':rbilea',
                            ':rrdb',
                            ':rfactor',
                            ':rbal',
                            ':vorsorge',
                            ':rbrs',
                            ':rbru',
                            ':rbileg',
                            ':rbhu',
                            ':rkag',
                            ':rlcz',
                            ':rbko',
                            ':kathrein',
                            ':rbcz',
                            ':rwbb',
                            ':tbsk'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                )
            ),
            'approval-authority', ':credit-committee',
            'limit-approach', ':bottom-up',
            'top-gcc', jsonb_build_object(
                'gics-level-3-long-name', format('Long Name %s', x),
                'top-parent-id', format('#%s', gen_random_uuid()),
                'short-name', format('Some Name %s', x),
                'top-gcc-id', format('#%s', gen_random_uuid()),
                'country-of-risk', 'AT',
                'gics-level-1-short-name', format('NAME-%s', x),
                'id', format('#%s', gen_random_uuid()),
                'gics-level-1-long-name', 'INDUSTRIALS',
                'cocunut', format('123%s', x),
                'gics-level-3-short-name', format('NAME-%s', x)
            ),
            'recommendation', jsonb_build_object(
                'role', ':lime-risk-managers',
                'recommendation-date', '2024-04-10T05:52:14.131Z',
                'recommendation', ':recommended',
                'id', format('user-%s@test.com', x)
            ),
            'assigned-rm', jsonb_build_object(
                'role', ':lime-risk-managers'
            ),
            'status', ':approved',
            'substatus', null,
            'credit-policy-compliance', jsonb_build_object(
                'compliance', ':not-calculated'
            ),
            'risk-on', jsonb_build_object(
                'top-parent-id', format('#%s', gen_random_uuid()),
                'short-name', format('Name %s', x),
                'entity-type', ':corporate',
                'top-parent-short-name', format('Some Name %s', x),
                'top-gcc-id', format('#%s', gen_random_uuid()),
                'country-of-risk', 'AT',
                'crs', jsonb_build_object(
                    'date-approved', '2024-01-01',
                    'risk-monitoring-unit', 'Y',
                    'client-risk-status', 'STD'
                ),
                'asset-class', jsonb_build_object(
                    'asset-class-name', 'Corporate',
                    'asset-class-code', '3'
                ),
                'segment', 'CO',
                'id', format('#%s', gen_random_uuid()),
                'cocunut', format('987%s', x),
                'top-gcc-short-name', format('Some Name %s', x),
                'cpar', '3A',
                'top-gcc-cocunut', format('6234%s', x),
                'country-of-domicile', 'AT',
                'rating', jsonb_build_object(
                    'balance-sheet-date', '2023-12-31',
                    'rating-note-country', '2A',
                    'derived-from-cocunut', format('312%s', x),
                    'rating-model', 'CORPL',
                    'derived-rating-flag', 'Y',
                    'delivered-rating-note-country', '2A',
                    'delivered-pd-rate-fcy', format('.000%s', x),
                    'delivered-rating-note-lcy', '3A',
                    'rating-type', 'internal',
                    'country-of-risk', 'AT',
                    'data-source', 'BCO',
                    'customer-segment-flag', 'NR',
                    'rating-note-lcy', '3A',
                    'rating-unit', 'RBI',
                    'derived-rating-note-country', '2A',
                    'derived-country-of-risk', 'AT',
                    'pd-rate-lcy', format('.000%s', x),
                    'business-date', '2024-03-28',
                    'rating-date', '2024-03-27',
                    'pd-rate-fcy', format('.000%s', x),
                    'delivered-rating-note-fcy', '3A',
                    'delivered-pd-rate-lcy', format('.000%s', x),
                    'rating-note-fcy', '3A'
                )
            ),
            'applicant', jsonb_build_object(
                'role', ':lime-account-managers',
                'id', format('user-%s@test.com', x),
                'attrs', jsonb_build_object(
                    'department', format('Some Department %s', x),
                    'department-code', format('DEP%s', x)
                ),
                'units', jsonb_build_array(
                    ':rbspk',
                    ':rbhr',
                    ':rbi',
                    ':rbby',
                    ':aval',
                    ':rbbh',
                    ':rbro',
                    ':rbilea',
                    ':rrdb',
                    ':rfactor',
                    ':rbal',
                    ':vorsorge',
                    ':rbrs',
                    ':rbru',
                    ':rbileg',
                    ':rbhu',
                    ':rkag',
                    ':rlcz',
                    ':rbko',
                    ':kathrein',
                    ':rbcz',
                    ':rwbb',
                    ':tbsk'
                ),
                'areas', jsonb_build_array(
                    ':all'
                )
            ),
            'area', ':corporate',
            'risk-statement', jsonb_build_object(
                'esg-risk-analysis', '',
                'underwriting-principles', '',
                'type', ':short',
                'financials', jsonb_build_array(),
                'industry-risk', jsonb_build_array(),
                'refinancing-risk', '',
                'special-risk-factors', '',
                'counter-party-description', '',
                'impairment-and-default', '',
                'recommendation', format('%s/%s', gen_random_uuid(), gen_random_uuid()),
                'financial-covenants', '',
                'conditions', jsonb_build_array(),
                'feasibility-cf-projections', '',
                'business-risk', jsonb_build_array(),
                'request-comment', '',
                'credit-policy', '',
                'compliance-information', '',
                'transaction-structure', ''
            ),
            'booking-company', 'RBI',
            'resolution-date', '2024-04-10T05:54:36.048Z',
            'creation-time', '2024-04-10T05:47:59.498Z',
            'decision', jsonb_build_object(
                'credit-committee', jsonb_build_object(
                    'decision', jsonb_build_object(
                        'credit-committee', jsonb_build_object(
                            'decision-date', '2024-04-10',
                            'circulation', ':no',
                            'type', ':decision',
                            'entered-at', '2024-04-10T05:53:39.168Z',
                            'status', ':positive',
                            'entered-by', jsonb_build_object(
                                'role', ':lime-verifiers',
                                'email', format('user-%s@test.com', x),
                                'decisionlevel', jsonb_build_array(
                                    ':professionalmanager'
                                ),
                                'roles', jsonb_build_array(
                                    ':lime-readers',
                                    ':lime-workout-managers',
                                    ':lime-product-managers',
                                    ':lime-limit-managers',
                                    ':lime-risk-managers',
                                    ':lime-account-managers',
                                    ':lime-tcm-releaser',
                                    ':lime-verifiers'
                                ),
                                'function', jsonb_build_array(
                                    ':business'
                                ),
                                'id', format('user-%s@test.com', x),
                                'attrs', jsonb_build_object(
                                    'department', format('Some Department %s', x),
                                    'department-code', format('DEP%s', x)
                                ),
                                'action', jsonb_build_array(
                                    ':switch-realm'
                                ),
                                'units', jsonb_build_array(
                                    ':rbspk',
                                    ':rbhr',
                                    ':rbi',
                                    ':rbby',
                                    ':aval',
                                    ':rbbh',
                                    ':rbro',
                                    ':rbilea',
                                    ':rrdb',
                                    ':rfactor',
                                    ':rbal',
                                    ':vorsorge',
                                    ':rbrs',
                                    ':rbru',
                                    ':rbileg',
                                    ':rbhu',
                                    ':rkag',
                                    ':rlcz',
                                    ':rbko',
                                    ':kathrein',
                                    ':rbcz',
                                    ':rwbb',
                                    ':tbsk'
                                ),
                                'areas', jsonb_build_array(
                                    ':all'
                                )
                            )
                        )
                    )
                )
            )
        ),
        'version', 38,
        'application-snapshots', jsonb_build_array(
            jsonb_build_object(
                'snapshot-id', format('#%s', gen_random_uuid()),
                'from-status', ':created',
                'to-status', ':risk-assessment',
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'snapshot-id', format('#%s', gen_random_uuid()),
                'from-status', ':risk-assessment',
                'to-status', ':waiting-decision',
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'snapshot-id', format('#%s', gen_random_uuid()),
                'from-status', ':waiting-decision',
                'to-status', ':verification',
                'as-of-date', '2024-04-10'
            ),
            jsonb_build_object(
                'snapshot-id', format('#%s', gen_random_uuid()),
                'from-status', ':verification',
                'to-status', ':approved',
                'as-of-date', '2024-04-10'
            )
        )
    ) as aggregate
from
    generate_series(1, 15000) as gen(x)
returning id;

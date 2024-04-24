----
-- glms-task-manager-svc, 33K on prod
----

insert into test_glms_task_manager_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'category', ':support',
            'assignees', jsonb_build_object(
                'roles', jsonb_build_array(
                    ':lime-risk-managers'
                ),
                'users', jsonb_build_array(
                    jsonb_build_object(
                        'role', ':lime-risk-managers',
                        'status', ':active',
                        'id', format('user-%s@rbinternational.com', x)
                    )
                )
            ),
            'gcc', jsonb_build_object(
                'short-name', format('generated/%s', x),
                'cocunut', format('%s', x)
            ),
            'application', jsonb_build_object(
                'application-id', x,
                'decision-type', ':approval',
                'id', format('#%s', gen_random_uuid()),
                'area', ':corporate',
                'booking-company', 'RBI'
            ),
            'transitions', jsonb_build_array(
                jsonb_build_object(
                    'from', ':created',
                    'action', ':do-not-support',
                    'to', ':not-supported'
                ),
                jsonb_build_object(
                    'from', ':created',
                    'action', ':support',
                    'to', ':supported'
                ),
                jsonb_build_object(
                    'from', ':created',
                    'action', ':cancel',
                    'to', ':cancelled'
                )
            ),
            'recipient', ':lime-risk-managers',
            'type', ':change-steering-approach',
            'journal', jsonb_build_array(
                jsonb_build_object(
                    'event-id', ':task-created',
                    'time', '2024-02-15T19:48:11.661Z',
                    'user', jsonb_build_object(
                        'role', ':lime-account-managers',
                        'email', 'john.smith@rbinternational.com',
                        'roles', jsonb_build_array(
                            ':lime-account-managers',
                            ':lime-product-managers',
                            ':lime-verifiers',
                            ':lime-tcm-releaser',
                            ':lime-workout-managers',
                            ':lime-readers',
                            ':lime-limit-managers',
                            ':lime-risk-managers'
                        ),
                        'id', 'john.smith@rbinternational.com',
                        'attrs', jsonb_build_object(
                            'department', 'No Department',
                            'department-code', '000'
                        ),
                        'units', jsonb_build_array(
                            ':rbhr',
                            ':rbbh',
                            ':habito',
                            ':rbi',
                            ':rbcz'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                ),
                jsonb_build_object(
                    'event-id', ':not-supported',
                    'time', '2024-02-15T19:48:12.111Z',
                    'user', jsonb_build_object(
                        'role', ':lime-risk-managers',
                        'email', 'john.smith@rbinternational.com',
                        'roles', jsonb_build_array(
                            ':lime-account-managers',
                            ':lime-product-managers',
                            ':lime-verifiers',
                            ':lime-tcm-releaser',
                            ':lime-workout-managers',
                            ':lime-readers',
                            ':lime-limit-managers',
                            ':lime-risk-managers'
                        ),
                        'id', 'john.smith@rbinternational.com',
                        'attrs', jsonb_build_object(
                            'department', 'No Department',
                            'department-code', '000'
                        ),
                        'units', jsonb_build_array(
                            ':rbhr',
                            ':rbbh',
                            ':habito',
                            ':rbi',
                            ':rbcz'
                        ),
                        'areas', jsonb_build_array(
                            ':all'
                        )
                    )
                )
            ),
            'created-by', jsonb_build_object(
                'role', ':lime-account-managers',
                'email', 'john.smith@rbinternational.com',
                'roles', jsonb_build_array(
                    ':lime-account-managers',
                    ':lime-product-managers',
                    ':lime-verifiers',
                    ':lime-tcm-releaser',
                    ':lime-workout-managers',
                    ':lime-readers',
                    ':lime-limit-managers',
                    ':lime-risk-managers'
                ),
                'id', 'john.smith@rbinternational.com',
                'attrs', jsonb_build_object(
                    'department', 'No Department',
                    'department-code', '000'
                ),
                'units', jsonb_build_array(
                    ':rbhr',
                    ':rbbh',
                    ':habito',
                    ':rbi',
                    ':rbcz'
                ),
                'areas', jsonb_build_array(
                    ':all'
                )
            ),
            'status', ':not-supported',
            'finished-date', format('2024-02-15T19:48:12.%sZ', x),
            'options', jsonb_build_object(
                'limit-approach', ':top-down'
            ),
            'finished-by', jsonb_build_object(
                'role', ':lime-risk-managers',
                'email', format('john.smith-%s@test.com', x),
                'roles', jsonb_build_array(
                    ':lime-account-managers',
                    ':lime-product-managers',
                    ':lime-verifiers',
                    ':lime-tcm-releaser',
                    ':lime-workout-managers',
                    ':lime-readers',
                    ':lime-limit-managers',
                    ':lime-risk-managers'
                ),
                'id', 'john.smith@rbinternational.com',
                'attrs', jsonb_build_object(
                    'department', 'No Department',
                    'department-code', format('%s', x)
                ),
                'units', jsonb_build_array(
                    ':rbhr',
                    ':rbbh',
                    ':habito',
                    ':rbi',
                    ':rbcz'
                ),
                'areas', jsonb_build_array(
                    ':all'
                )
            ),
            'creation-time', format('2024-02-15T19:48:11.%sZ', x)
        ),
        'version', 2
    ) as aggregate
from
    generate_series(1, 50000) as gen(x)
returning id;

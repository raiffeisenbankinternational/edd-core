----
-- glms-notification-svc, 89K on prod
----

insert into test_glms_notification_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'arguments', jsonb_build_object(
                'gcc', jsonb_build_object(
                    'short-name', format('Some Name %s', x),
                    'cocunut', format('123%s', x)
                ),
                'application', jsonb_build_object(
                    'sequence-id', x,
                    'id', format('#%s', gen_random_uuid())
                )
            ),
            'type', ':application-approved',
            'status', ':created',
            'user', jsonb_build_object(
                'role', ':lime-tcm-releaser',
                'given_name', format('Name-%s', x),
                'department', format('Some Department %s', x),
                'department-code', format('DEP%s', x),
                'roles', jsonb_build_array(
                    ':lime-tcm-releaser',
                    ':lime-limit-managers'
                ),
                'username', format('user-%s@rbinternational.com', x),
                'user-attributes', jsonb_build_object(
                    'sub', gen_random_uuid(),
                    'email_verified', 'false',
                    'cognito:user_status', 'EXTERNAL_PROVIDER',
                    'custom:user_id', format('#%s', gen_random_uuid()),
                    'identities', jsonb_build_array(jsonb_build_object(
                        'userId', format('user-%s@rbinternational.com', x),
                        'providerName', 'PingFederate',
                        'providerType', 'OIDC',
                        'issuer', null,
                        'primary', true,
                        'dateCreated', 1617191649730 + x
                    ))::text,
                    'profile', jsonb_build_array(
                        'RBI-GLMS-P-Areas-SOV',
                        'RBI-GLMS-P-Areas-Country',
                        'RBI-GLMS-P-Units-RBKO',
                        'RBI-GLMS-P-Units-TBSK',
                        'RBI-GLMS-P-Units-RBRO',
                        'RBI-GLMS-P-Units-RBHR',
                        'RBI-GLMS-P-Units-AVAL',
                        'RBI-GLMS-P-Units-RBSPK',
                        'RBI-GLMS-P-Units-RBRU',
                        'RBI-GLMS-P-Units-VORSORGE',
                        'RBI-GLMS-P-Units-RKAG',
                        'RBI-GLMS-P-Units-RBBY',
                        'RBI-GLMS-P-Units-RBRS',
                        'RBI-GLMS-P-Units-RBHU',
                        'RBI-GLMS-P-Units-RBCZ',
                        'RBI-GLMS-P-Units-RBBH',
                        'RBI-GLMS-P-Units-RBAL',
                        'RBI-GLMS-P-Units-RBI',
                        'RBI-GLMS-P-Function-Business',
                        'RBI-GLMS-P-Areas-FI',
                        'RBI-GLMS-P-Lime-TCM-Releaser',
                        'RBI-GLMS-P-Action-Switch-Realm',
                        'RBI-GLMS-P-Realm-Prod',
                        'RBI-GLMS-P-Lime-Limit-Managers'
                    )
                )::text,
                'family_name', format('FooBar%s', x),
                'department_code', format('RMO%s', x),
                'id', format('user-%s@rbinternational.com', x),
                'code', format('CODE%s', x),
                'full_name', format('Test User %s', x),
                'full_name_lower', format('test user %s', x)
            ),
            'creation-time', format('2023-09-15T09:40:04.%sZ', x),
            'send-email?', true
        ),
        'version', 1
    ) as aggregate
from
    generate_series(1, 150000) as gen(x)
returning id;

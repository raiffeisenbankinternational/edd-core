----
-- glms-user-management-svc, 4K on prod
----

insert into test_glms_user_management_svc.aggregates (id, aggregate)
select
    gen_random_uuid() as id,
    jsonb_build_object(
        '__generated__', true,
        'id', format('#%s', gen_random_uuid()),
        'attrs', jsonb_build_object(
            'given_name', format('user-%s', x),
            'department', null,
            'email', format('user_%s@generated.com', x),
            'roles', jsonb_build_array(
                ':lime-risk-managers'
            ),
            'username', format('generated_%s_test@rbinternational.com', x),
            'family_name', format('test-%s', x),
            'full_name', format('Name-%s Test-%s', x, x),
            'full_name_lower', format('name-%s test-%s', x, x)
        ),
        'state', ':active',
        'version', 1
    ) as aggregate
from
    generate_series(1, 5000) as gen(x)
returning id;

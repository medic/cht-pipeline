SELECT person.uuid,
		person.phone,
		person.phone2,
		person.date_of_birth,
		person.reported,
		parent.type AS parent_type
	FROM {{ ref("contactview_metadata") }} person
	LEFT JOIN {{ ref("contactview_metadata") }} parent ON person.parent_uuid = parent.uuid
	WHERE person.type = 'person'::text
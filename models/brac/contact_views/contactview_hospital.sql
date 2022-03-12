SELECT cmd.uuid,
    cmd.name
   FROM {{ ref("contactview_metadata") }} cmd
  WHERE cmd.type = 'district_hospital'::text;
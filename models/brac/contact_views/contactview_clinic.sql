SELECT cmd.uuid,
    cmd.name,
    chw.uuid AS chw_uuid,
    cmd.reported AS created
   FROM {{ ref("contactview_metadata") }} cmd
     JOIN {{ ref("contactview_chw") }} chw ON cmd.parent_uuid = chw.area_uuid
  WHERE cmd.type = 'clinic'::text
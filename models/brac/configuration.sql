{{ config(materialized = 'raw_sql') }}  

CREATE TABLE configuration IF NOT EXISTS (key TEXT, value JSONB);

INSERT INTO configuration VALUES
	
	/* General */

	-- Used in useview_visits
	('general', '{ "visits": {
		"can_refer_to": ["pregnancy","pregnancy_visit","pregnancy_referral_follow_up","delivery","postnatal_care"],
		"anc": ["pregnancy_visit"],
		"pnc": ["postnatal_care"]
		 } }'),

	/* ANC */

	-- Used in ancview_pregnancy
	('anc', '{ "lmp_calcs": {
		"expected_days_pregnant": 280,
		"maximum_days_pregnant": 294,
		"early_reg_in_days_since_lmp": 84,
		"days_since_lmp_when_none_provided": 28
		 } }'),


	-- Used in ancview_delivery
	('anc', '{ "pregnancy_outcomes": {
		"delivered": ["healthy","still_birth"],
		"sms_default": "healthy" } }'),

	-- Used in get_dashboard_data_anc_impact
	('anc', '{ "active": 
		["pregnancy","pregnancy_referral_follow_up","pregnancy_visit", "delivery"] }'),
		
	('pnc', '{ "active": 
		["postnatal_care"] }')
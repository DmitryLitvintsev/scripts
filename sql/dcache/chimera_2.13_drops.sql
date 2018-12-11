DROP TRIGGER IF EXISTS tgr_push_tag ON t_tags;
DROP TRIGGER IF EXISTS tgr_insert_al ON t_access_latency;
DROP TRIGGER IF EXISTS tgr_insert_rp ON t_retention_policy;
DROP TRIGGER IF EXISTS tgr_enstore_level2 ON t_level_2;

DROP FUNCTION f_push_tag(character varying, character varying);
DROP FUNCTION push_tag(character varying, character varying);
DROP FUNCTION f_push_tag();

DROP FUNCTION f_create_tag(character varying, character varying);
DROP FUNCTION f_dir_size(character varying);
DROP FUNCTION f_enstore_add_to_level2();
DROP FUNCTION f_fill_al();
DROP FUNCTION f_fill_rp();
DROP FUNCTION update_tag(character varying, character varying, character varying, integer);